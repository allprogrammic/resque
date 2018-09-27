<?php

/*
 * This file is part of the AllProgrammic Resque package.
 *
 * (c) AllProgrammic SAS <contact@allprogrammic.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace AllProgrammic\Component\Resque;

use AllProgrammic\Component\Resque\Events\QueueEvent;
use AllProgrammic\Component\Resque\Job\InvalidRecurringJobException;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;
use AllProgrammic\Component\Resque\Events\JobEvent;
use AllProgrammic\Component\Resque\Events\JobFailEvent;
use AllProgrammic\Component\Resque\Events\WorkerEvent;
use AllProgrammic\Component\Resque\Failure\FailureInterface;
use AllProgrammic\Component\Resque\Job\DirtyExitException;
use AllProgrammic\Component\Resque\Job\DontPerform;
use AllProgrammic\Component\Resque\Job\Status;
use Symfony\Component\EventDispatcher\EventDispatcherInterface;

class Worker
{
    /**
     * @var Engine
     */
    private $engine;

    /**
     * @var EventDispatcherInterface
     */
    private $dispatcher;

    /**
     * @var FailureInterface
     */
    private $failureHandler;

    /**
     * @var Lock
     */
    private $delayedLock;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var array Array of all associated queues for this worker.
     */
    private $queues = array();

    /**
     * @var bool
     */
    private $cyclic = false;

    /**
     * @var array Array of all cyclyc associated queues for this worker.
     */
    private $cyclicQueues = array();

    /**
     * @var string The hostname of this worker.
     */
    private $hostname;

    /**
     * @var boolean True if on the next iteration, the worker should shutdown.
     */
    private $shutdown = false;

    /**
     * @var boolean True if this worker is paused.
     */
    private $paused = false;

    /**
     * @var string String identifying this worker.
     */
    private $id;

    /**
     * @var Job Current job, if any, being processed by this worker.
     */
    private $currentJob = null;

    /**
     * @var int Process ID of child worker processes.
     */
    private $child = null;

    /**
     * @var int Process Id of child hearbeeat worker process.
     */
    private $childHeartbeat = null;

    /**
     * @var bool
     */
    private $isChild = false;

    /**
     * @var array|null
     */
    private $recurringJobs = null;

    /**
     * @var array|null
     */
    private $cleanerTasks = null;

    /**
     * @var int Interval to sleep for between checking schedules.
     */
    const SLEEP_INTERVAL = 5;

    /**
     * Instantiate a new worker, given a list of queues that it should be working
     * on. The list of queues should be supplied in the priority that they should
     * be checked for jobs (first come, first served)
     *
     * Passing a single '*' allows the worker to work on all queues in alphabetical
     * order. You can easily add new queues dynamically and have them worked on using
     * this method.
     *
     * @param Engine $engine
     * @param EventDispatcherInterface $dispatcher
     * @param FailureInterface $failureHandler
     * @param Lock $delayedLock
     * @param string|array $queues String with a single queue name, array with multiple.
     * @param LoggerInterface $logger
     */
    public function __construct(
        Engine $engine,
        Heart $heart,
        EventDispatcherInterface $dispatcher,
        FailureInterface $failureHandler,
        Lock $delayedLock,
        $queues,
        $cyclic = false,
        LoggerInterface $logger = null
    ) {
        $this->engine = $engine;
        $this->heart = $heart;
        $this->dispatcher = $dispatcher;
        $this->failureHandler = $failureHandler;
        $this->delayedLock = $delayedLock;
        $this->logger = $logger;

        if (!is_array($queues)) {
            $queues = [$queues];
        }

        $this->cyclic = $cyclic;
        $this->queues = $queues;
        $this->hostname = Engine::getHostname();

        $this->id = implode(':', [
            $this->hostname,
            getmypid(),
            implode(',', $this->queues)
        ]);
    }

    /**
     * Set the ID of this worker to a given ID string.
     *
     * @param string $workerId ID for the worker.
     */
    public function setId($workerId)
    {
        $this->id = $workerId;
    }

    public function getId()
    {
        return $this->id;
    }

    /**
     * @return Job
     */
    public function getJob()
    {
        $job = $this->engine->getBackend()->get(sprintf('worker:%s', $this->id));

        if (!$job) {
            return null;
        }

        return json_decode($job, true);
    }

    public function isIdle()
    {
        return !$this->getJob();
    }

    public function getHost()
    {
        list($hostname, $pid, $queues) = explode(':', $this->getId(), 3);

        return $hostname;
    }

    public function getPid()
    {
        list($hostname, $pid, $queues) = explode(':', $this->getId(), 3);

        return $pid;
    }

    public function getStartedAt()
    {
        return $this->engine->getStartedAt($this->id);
    }

    public function getProcessed()
    {
        return $this->engine->getProcessedStat($this->id);
    }

    public function getFailed()
    {
        return $this->engine->getFailedStat($this->id);
    }

    public function getHeartbeat()
    {
        return $this->engine->getHeartbeat($this->id);
    }

    public function getDelayedLock()
    {
        return $this->delayedLock;
    }

    /**
     * The primary loop for a worker which when called on an instance starts
     * the worker's life cycle.
     *
     * Queues are checked every $interval (seconds) for new jobs.
     *
     * @param int $interval How often to check for new jobs across the queues.
     * @param bool $blocking
     */
    public function work($interval = self::SLEEP_INTERVAL, $blocking = false)
    {
        $this->updateProcLine('Starting');
        $this->startup();

        while (true) {
            pcntl_signal_dispatch();

            if ($this->shutdown) {
                break;
            }

            $this->handleCleanerTasks();
            $this->handleDelayedItems();
            $this->handleRecurredItems();

            $job = false;

            // Attempt to find and reserve a job
            if (!$this->paused) {
                if ($blocking === true) {
                    $this->updateProcLine(sprintf(
                        'Waiting for %s with blocking timeout %s',
                        implode(',', $this->queues),
                        $interval
                    ));
                } else {
                    $this->updateProcLine(sprintf(
                        'Waiting for %s with interval %s',
                        implode(',', $this->queues),
                        $interval
                    ));
                }

                $job = $this->reserve($blocking, $interval);
            }

            if (!$job || !($job instanceof Job)) {
                // For an interval of 0, break now - helps with unit testing etc
                if ($interval == 0) {
                    break;
                }

                if ($blocking === false) {
                    // If no job was found, we sleep for $interval before continuing and checking again
                    $this->log(LogLevel::INFO, sprintf('Sleeping for %s', $interval));
                    if ($this->paused) {
                        $this->updateProcLine('Paused');
                    } else {
                        $this->updateProcLine('Waiting for ' . implode(',', $this->queues));
                    }

                    usleep($interval * 1000000);
                }

                continue;
            }

            $this->log(LogLevel::NOTICE, sprintf('Starting work on %s', $job));
            $this->dispatcher->dispatch(ResqueEvents::BEFORE_FORK, new JobEvent($job));

            $this->workingOn($job);

            $this->child = $this->engine->fork();

            if ($this->child === -1) {
                $this->log(LogLevel::ERROR, 'Unable to fork');

                exit(-1);
            }

            // Forked and we're the child. Run the job.
            if ($this->child === 0 || $this->child === false) {
                $this->isChild = true;

                $this->updateProcLine($status = sprintf('Recurring %s since %s', $job->getQueue(), strftime('%F %T')));
                $this->log(LogLevel::INFO, $status);

                $this->engine->performRecurringJobs($job);

                $this->updateProcLine($status = sprintf('Processing %s since %s', $job->getQueue(), strftime('%F %T')));
                $this->log(LogLevel::INFO, $status);

                $this->perform($job);

                $this->updateProcLine($status = sprintf('Ending job %s since %s', $job->getQueue(), strftime('%F %T')));
                $this->log(LogLevel::INFO, $status);
                
                exit(0);
            }

            if ($this->child > 0) {
                // Parent process, sit and wait
                $this->updateProcLine($status = sprintf('Forked %s at %s', $this->child, strftime('%F %T')));
                $this->log(LogLevel::INFO, $status);

                // Wait until the child process finishes before continuing
                pcntl_wait($status);
                $exitStatus = pcntl_wexitstatus($status);

                if ($exitStatus !== 0) {
                    $this->jobFail($job, new DirtyExitException(sprintf('Job exited with exit code %s', $exitStatus)));
                }
            }

            $this->child = null;
            $this->doneWorking();
        }

        $this->engine->unregisterWorker($this);
    }

    /**
     * Process a single job.
     *
     * @param Job $job The job to be processed.
     */
    public function perform(Job $job)
    {
        try {
            $this->dispatcher->dispatch(ResqueEvents::AFTER_FORK, new JobEvent($job));
            $this->dispatcher->dispatch(ResqueEvents::JOB_BEFORE_PERFORM, new JobEvent($job));

            $job->perform();

            $this->dispatcher->dispatch(ResqueEvents::JOB_AFTER_PERFORM, new JobEvent($job));
        } catch (DontPerform $e) {
            return $this->jobFail($job, $e);
        } catch (\Exception $e) {
            return $this->jobFail($job, $e);
        } catch (\Throwable $e) {
            return $this->jobFail($job, $e);
        }

        $job->updateStatus(Status::STATUS_COMPLETE);
        $this->log(LogLevel::NOTICE, sprintf('%s has finished', $job));
    }

    protected function jobFail(Job $job, $exception)
    {
        $this->log(LogLevel::CRITICAL, sprintf('% shas failed %s', $job, $exception));

        $this->dispatcher->dispatch(ResqueEvents::JOB_FAILURE, new JobFailEvent($job, $exception));

        $id = $job->fail($this->failureHandler, $exception);

        $job->setFailed($id - 1);

        $this->updateJobStatus($job, Status::STATUS_FAILED);
        $this->engine->statFailed($this);
    }

    protected function jobFailHeartbeat()
    {
        $job = $this->engine->getBackend()->get(sprintf('worker:%s', $this->id));
        $job = json_decode($job, true);

        if (!$job) {
            return false;
        }

        $job = new Job($job['queue'], $job['payload'], $this);

        $this->jobFail($job, new DirtyExitException());
    }

    public function updateJobStatus(Job $job, $status)
    {
        if (!$job->getId()) {
            return;
        }

        $this->engine->updateRecurringJobStatus($job, $status);
        $this->engine->updateJobStatus($job->getId(), $status);
    }

    /**
     * @param  bool            $blocking
     * @param  int             $timeout
     * @return object|boolean Instance of Resque_Job if a job is found, false if not.
     */
    public function reserve($blocking = false, $timeout = null)
    {
        if (!is_array($queues = $this->queues())) {
            return false;
        }

        if (count($queues) == 0) {
            return false;
        }

        if ($blocking === true) {
            $this->log(LogLevel::INFO, sprintf('Starting blocking with timeout of %s', $timeout));

            if ($job = $this->engine->reserveBlocking($queues, $timeout)) {
                $this->log(LogLevel::INFO, sprintf('Found job on %s', $job->getQueue()));

                return $job;
            }
        } else {
            foreach ($queues as $queue) {
                $this->log(LogLevel::INFO, sprintf('Checking %s for jobs', $queue));

                if ($job = $this->engine->reserve($queue)) {
                    $this->log(LogLevel::INFO, sprintf('Found job on %s', $job->getQueue()));

                    return $job;
                }
            }
        }

        return false;
    }

    /**
     * Return an array containing all of the queues that this worker should use
     * when searching for jobs.
     *
     * If * is found in the list of queues, every queue will be searched in
     * alphabetic order. (@see $fetch)
     *
     * @param boolean $fetch If true, and the queue is set to *, will fetch
     * all queue names from redis.
     * @return array Array of associated queues.
     */
    public function queues($fetch = true)
    {
        if (!in_array('*', $this->queues) || $fetch == false) {
            if (count($this->queues) === 1 || !$this->cyclic) {
                return $this->queues;
            }

            return $this->handleCyclicMode($this->queues);
        }

        $queues = $this->engine->queues();
        $queues = $this->handleCyclicMode($queues);

        if (!$this->cyclic) {
            sort($queues);
        }

        return $queues;
    }

    /**
     * @param $queues
     *
     * @return array
     */
    public function handleCyclicMode($queues)
    {
        if (!$this->cyclic) {
            return $queues;
        }

        if (!empty(array_diff($queues, $this->cyclicQueues))) {
            $this->cyclicQueues = $queues;
        }

        $queue = array_shift($this->cyclicQueues);
        array_push($this->cyclicQueues, $queue);

        return $this->cyclicQueues;
    }

    /**
     * @param $queue
     * @return bool
     */
    public function matchQueues($queue)
    {
        if (in_array('*', $this->queues)) {
            return true;
        }

        return in_array($queue, $this->queues);
    }

    /**
     * Perform necessary actions to start a worker.
     */
    private function startup()
    {
        $this->registerSigHandlers();
        $this->engine->pruneDeadWorkers();
        $this->engine->pruneDeadWorkersHeartbeat();

        $this->dispatcher->dispatch(ResqueEvents::BEFORE_FIRST_FORK, new WorkerEvent($this));

        $this->engine->registerWorker($this);
        $this->heartbeat();
    }

    /**
     * On supported systems (with the PECL proctitle module installed), update
     * the name of the currently running process to indicate the current state
     * of a worker.
     *
     * @param string $status The updated process title.
     */
    private function updateProcLine($status)
    {
        $processTitle = sprintf('resque: %s', $status);

        if (function_exists('cli_set_process_title') && PHP_OS !== 'Darwin') {
            cli_set_process_title($processTitle);
        } elseif (function_exists('setproctitle')) {
            setproctitle($processTitle);
        }
    }

    /**
     * Register signal handlers that a worker should respond to.
     *
     * TERM: Shutdown immediately and stop processing jobs.
     * INT: Shutdown immediately and stop processing jobs.
     * QUIT: Shutdown after the current job finishes processing.
     * USR1: Kill the forked child immediately and continue processing jobs.
     */
    private function registerSigHandlers()
    {
        // Restore exception handler
        restore_exception_handler();

        // Register shutdown function
        register_shutdown_function([$this, 'phpShutdown']);

        $this->log(LogLevel::DEBUG, 'Registered signals');

        if (!function_exists('pcntl_signal')) {
            return;
        }

        pcntl_signal(SIGTERM, [$this, 'shutDownNow']);
        pcntl_signal(SIGINT,  [$this, 'shutDownNow']);
        pcntl_signal(SIGQUIT, [$this, 'shutdown']);
        pcntl_signal(SIGUSR1, [$this, 'killChild']);
        pcntl_signal(SIGUSR2, [$this, 'pauseProcessing']);
        pcntl_signal(SIGCONT, [$this, 'unPauseProcessing']);
    }

    /**
     * Signal handler callback for USR2, pauses processing of new jobs.
     */
    public function pauseProcessing()
    {
        $this->log(LogLevel::NOTICE, 'USR2 received; pausing job processing');

        $this->paused = true;
    }

    /**
     * Signal handler callback for CONT, resumes worker allowing it to pick
     * up new jobs.
     */
    public function unPauseProcessing()
    {
        $this->log(LogLevel::NOTICE, 'CONT received; resuming job processing');

        $this->paused = false;
    }

    /**
     * Schedule a worker for shutdown. Will finish processing the current job
     * and when the timeout interval is reached, the worker will shut down.
     */
    public function shutdown()
    {
        $this->log(LogLevel::NOTICE, 'Shutting down...');

        $this->shutdown = true;
    }

    /**
     * Force an immediate shutdown of the worker, killing any child jobs
     * currently running.
     */
    public function shutdownNow()
    {
        $this->log(LogLevel::DEBUG, 'shutdownNow');

        $this->shutdown();
        $this->killChild();
        $this->killHeartbeat();
    }

    public function phpShutdown()
    {
        $error = error_get_last();

        if (is_object($this->currentJob) && isset($error['message'])) {
            $this->currentJob->fail($this->failureHandler,
                new DirtyExitException($error['message'])
            );
        }

        if ($this->isChild) {
            return;
        }

        $this->log(LogLevel::DEBUG, 'phpShutdown');

        $this->shutdown();
        $this->killChild();
        $this->killHeartbeat();

        $this->engine->unregisterWorker($this);
    }

    /**
     * Kill a forked child job immediately. The job it is processing will not
     * be completed.
     */
    public function killChild()
    {
        if (!$this->child) {
            $this->log(LogLevel::DEBUG, 'No child to kill.');

            return;
        }

        $this->log(LogLevel::INFO, sprintf('Killing child at %s', $this->child));

        if (exec('ps -o pid -p ' . $this->child, $output, $returnCode) && $returnCode != 1) {
            $this->log(LogLevel::DEBUG, sprintf('Child %s found, killing.', $this->child));

            posix_kill($this->child, SIGKILL);
            $this->child = null;
        } else {
            $this->log(LogLevel::INFO, sprintf('Child %s not found, stopping.', $this->child));

            $this->shutdown();
        }
    }

    /**
     * Kill a forked heartbeat job immediately. The job it is processing will not
     * be completed.
     */
    public function killHeartbeat()
    {
        if (!$this->childHeartbeat) {
            $this->log(LogLevel::DEBUG, 'No child to kill.');

            return;
        }

        $this->log(LogLevel::INFO, sprintf('Killing child at %s', $this->childHeartbeat));

        if (exec('ps -o pid -p ' . $this->childHeartbeat, $output, $returnCode) && $returnCode != 1) {
            $this->log(LogLevel::DEBUG, sprintf('Child %s found, killing.', $this->childHeartbeat));

            posix_kill($this->childHeartbeat, SIGKILL);
            $this->childHeartbeat = null;
        } else {
            $this->log(LogLevel::INFO, sprintf('Child %s not found, stopping.', $this->childHeartbeat));

            $this->shutdown();
        }
    }

    public function unregister()
    {
        if (is_object($this->currentJob)) {
            $this->jobFail($this->currentJob, new DirtyExitException);
        } else {
            $this->jobFailHeartbeat();
        }
    }

    /**
     * Tell Redis which job we're currently working on.
     *
     * @param Job $job Resque_Job instance containing the job we're working on.
     */
    public function workingOn(Job $job)
    {
        $job->setWorker($this);

        $this->currentJob = $job;

        $job->updateStatus(Status::STATUS_RUNNING);

        $this->engine->updateWorkerJob($this, $job);
    }

    /**
     * Notify Redis that we've finished working on a job, clearing the working
     * state and incrementing the job stats.
     */
    public function doneWorking()
    {
        $this->engine->updateWorkerJob($this, $this->currentJob = null);
    }

    /**
     * Generate a string representation of this worker.
     *
     * @return string String identifier for this worker instance.
     */
    public function __toString()
    {
        return $this->id;
    }

    /**
     * @param $level
     * @param $message
     * @param array $context
     */
    private function log($level, $message, array $context = [])
    {
        if (!$this->logger) {
            return;
        }

        $this->logger->log($level, $message, $context);
    }

    public function getService($id)
    {
        return $this->engine->getService($id);
    }

    /**
     * Handle cleaner tasks
     */
    public function handleCleanerTasks()
    {
        $this->cleanerTasks = $this->engine->getCleaner()->peek(0, 0);

        foreach ($this->cleanerTasks as $key => $result) {
            $cleaner = new Cleaner($this->engine, $result);
            $cleaner->handle();
        }
    }

    /**
     * Handle delayed items for the next scheduled timestamp.
     *
     * Searches for any items that are due to be scheduled in Resque
     * and adds them to the appropriate job queue in Resque.
     *
     * @param DateTime|int $timestamp Search for any items up to this timestamp to schedule.
     */
    public function handleDelayedItems($timestamp = null)
    {
        while (($oldestJobTimestamp = $this->engine->nextDelayedTimestamp($timestamp)) !== false) {
            $this->updateProcLine('Processing Delayed Items');
            $this->enqueueDelayedItemsForTimestamp($oldestJobTimestamp);
        }
    }

    /**
     * Handle recurred items for the next interval.
     *
     * @return
     */
    public function handleRecurredItems()
    {
        $this->recurringJobs = $this->engine->getRecurring()->peek(0, 0);

        foreach ($this->recurringJobs as $key => $result) {
            if (!$this->matchQueues($result['queue'])) {
                continue;
            }

            $job = new RecurringJob(
                $this->engine,
                $this,
                $result['queue'],
                json_decode($result['args'], true),
                $result['class']
            );

            $job->setName($result['name']);
            $job->setDescription($result['description']);
            $job->setExpression($result['cron']);
            $job->setActive((isset($result['active']) ? $result['active'] : true));
            $job->schedule();
        }
    }

    /**
     * Schedule all of the delayed jobs for a given timestamp.
     *
     * Searches for all items for a given timestamp, pulls them off the list of
     * delayed jobs and pushes them across to Resque.
     *
     * @param \DateTime|int $timestamp Search for any items up to this timestamp to schedule.
     */
    public function enqueueDelayedItemsForTimestamp($timestamp)
    {
        if (!$this->delayedLock->lock()) {
            return false;
        }

        while ($item = $this->engine->nextItemForTimestamp($timestamp)) {

            $this->log(LogLevel::INFO, sprintf('Queueing %s in %s [delayed]', $item['class'], $item['queue']));

            $id = Engine::generateJobId();
            $this->dispatcher->dispatch(ResqueEvents::BEFORE_DELAYED_ENQUEUE, new QueueEvent($item['class'], $item['args'], $item['queue'], $id));

            $this->engine->enqueue($item['queue'], $item['class'], $item['args']);
        }

        $this->delayedLock->release();
    }

    /**
     * Start hearbit
     *
     * @param int $interval
     */
    public function heartbeat($interval = self::SLEEP_INTERVAL)
    {
        $parentPid = getmypid();
        $this->childHeartbeat = $this->engine->fork();

        if ($this->childHeartbeat === -1) {
            $this->log(LogLevel::ERROR, 'Unable to fork');

            exit(-1);
        }

        // Forked and we're the child. Run the job.
        if ($this->childHeartbeat === 0 || $this->childHeartbeat === false) {
            $this->updateProcLine('Heartbeat');

            while ($this->engine->isWorkerLive($parentPid)) {
                pcntl_signal_dispatch();

                if ($this->shutdown) {
                    break;
                }

                $this->heart->beat($this);
                $this->engine->pruneDeadWorkersHeartbeat();
                $this->engine->keepWorker($this);

                sleep($interval);
            }

            $this->engine->unregisterWorker($this);
            exit;
        }
    }
}
