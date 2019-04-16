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

use AllProgrammic\Component\Resque\Charts\Charts;
use AllProgrammic\Component\Resque\Delayed\DelayedInterface;
use AllProgrammic\Component\Resque\Events\DelayedEvent;
use AllProgrammic\Component\Resque\Job\DirtyExitException;
use AllProgrammic\Component\Resque\Job\InvalidTimestampException;
use AllProgrammic\Component\Resque\Processed\ProcessedInterface;
use AllProgrammic\Component\Resque\Cleaner\CleanerInterface;
use AllProgrammic\Component\Resque\Recurring\RecurringInterface;
use PhpSpec\Wrapper\DelayedCall;
use Psr\Log\LoggerInterface;
use AllProgrammic\Component\Resque\Events\QueueEvent;
use AllProgrammic\Component\Resque\Failure\FailureInterface;
use AllProgrammic\Component\Resque\Job\DontCreate;
use AllProgrammic\Component\Resque\Job\Status;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Symfony\Component\EventDispatcher\EventDispatcherInterface;

class Engine
{
    /** @var Redis */
    private $backend;

    /** @var EventDispatcherInterface */
    private $dispatcher;

    /** @var ContainerInterface */
    private $container;

    /** @var FailureInterface */
    private $failureHandler;

    /** @var DelayedInterface */
    private $delayedHandler;

    /** @var RecurringInterface */
    private $recurringHandler;

    /** @var CleanerInterface */
    private $cleanerHandler;

    /** @var Charts */
    private $charts;

    /** @var Lock */
    private $delayedLock;

    /** @var Stat */
    private $stat;

    /** @var Status */
    private $statusManager;

    /** @var Supervisor */
    private $supervisor;

    /** @var Heart */
    private $heart;

    /**
     * Engine constructor.
     *
     * @param Redis $backend
     * @param EventDispatcherInterface $dispatcher
     * @param ContainerInterface $container
     * @param Stat $stat
     * @param Status $statusManager
     * @param Heart $heart
     * @param FailureInterface $failureHandler
     * @param DelayedInterface $delayedHandler
     * @param RecurringInterface $recurringHandler
     * @param CleanerInterface $cleanerHandler
     * @param Charts $charts
     * @param Lock $delayedLock
     * @param LoggerInterface|null $logger
     */
    public function __construct(
        Redis $backend,
        EventDispatcherInterface $dispatcher,
        ContainerInterface $container,
        Stat $stat,
        Status $statusManager,
        Heart $heart,
        FailureInterface $failureHandler,
        DelayedInterface $delayedHandler,
        RecurringInterface $recurringHandler,
        CleanerInterface $cleanerHandler,
        Charts $charts,
        Lock $delayedLock,
        LoggerInterface $logger = null
    ) {
        $this->backend = $backend;
        $this->container = $container;
        $this->dispatcher = $dispatcher;
        $this->stat = $stat;
        $this->statusManager = $statusManager;
        $this->heart = $heart;
        $this->failureHandler = $failureHandler;
        $this->delayedHandler = $delayedHandler;
        $this->recurringHandler = $recurringHandler;
        $this->cleanerHandler = $cleanerHandler;
        $this->charts = $charts;
        $this->delayedLock = $delayedLock;

        $this->supervisor = new Supervisor($this, $this->backend, $heart, $dispatcher, $failureHandler, $delayedLock, $logger);
    }

    /**
     * @return FailureInterface
     */
    public function getFailure()
    {
        return $this->failureHandler;
    }

    /**
     * @return DelayedInterface
     */
    public function getDelayed()
    {
        return $this->delayedHandler;
    }

    /**
     * @return RecurringInterface
     */
    public function getRecurring()
    {
        return $this->recurringHandler;
    }

    /**
     * @return CleanerInterface
     */
    public function getCleaner()
    {
        return $this->cleanerHandler;
    }

    /**
     * fork() helper method for php-resque that handles issues PHP socket
     * and phpredis have with passing around sockets between child/parent
     * processes.
     *
     * Will close connection to Redis before forking.
     *
     * @return int Return vars as per pcntl_fork()
     */
    public function fork()
    {
        if (!function_exists('pcntl_fork')) {
            return -1;
        }

        // Close the connection to Redis before forking.
        // This is a workaround for issues phpredis has.
        $this->backend->forceClose();

        $pid = pcntl_fork();
        if ($pid === -1) {
            throw new \RuntimeException('Unable to fork child worker.');
        }

        return $pid;
    }

    /**
     * Push a job to the end of a specific queue. If the queue does not
     * exist, then create it as well.
     *
     * @param string $queue The name of the queue to add the job to.
     * @param array $item Job description as an array to be JSON encoded.
     *
     * @return bool
     */
    public function push($queue, $item)
    {
        $encodedItem = json_encode($item);

        if ($encodedItem === false) {
            return false;
        }

        $this->backend->sAdd('queues', $queue);
        $length = $this->backend->rPush('queue:' . $queue, $encodedItem);

        if ($length < 1) {
            return false;
        }
        return true;
    }

    /**
     * Pop an item off the end of the specified queue, decode it and
     * return it.
     *
     * @param string $queue The name of the queue to fetch an item from.
     * @return array Decoded item from the queue.
     */
    public function pop($queue)
    {
        $item =  $this->backend->lPop('queue:' . $queue);

        if (!$item) {
            return null;
        }

        return json_decode($item, true);
    }

    /**
     * Remove items of the specified queue
     *
     * @param string $queue The name of the queue to fetch an item from.
     * @param array $items
     * @return integer number of deleted items
     */
    public function dequeue($queue, $items = array())
    {
        if (count($items) > 0) {
            return $this->removeItems($queue, $items);
        }

        return $this->removeList($queue);
    }

    /**
     * Remove specified queue
     *
     * @param string $queue The name of the queue to remove.
     * @return integer Number of deleted items
     */
    public function removeQueue($queue)
    {
        $num = $this->removeList($queue);

        $this->backend->sRem('queues', $queue);

        return $num;
    }

    /**
     * Pop an item off the end of the specified queues, using blocking list pop,
     * decode it and return it.
     *
     * @param array         $queues
     * @param int           $timeout
     * @return null|array   Decoded item from the queue.
     */
    public function blpop(array $queues, $timeout)
    {
        $list = array();
        foreach ($queues as $queue) {
            $list[] = 'queue:' . $queue;
        }

        $item = $this->backend->blPop($list, (int)$timeout);

        if (!$item) {
            return null;
        }

        /**
         * Normally the Redis class returns queue names without the prefix
         * But the blpop is a bit different. It returns the name as prefix:queue:name
         * So we need to strip off the prefix:queue: part
         */
        $queue = substr($item[0], strlen($this->backend->getNamespace() . 'queue:'));

        return [
            'queue'   => $queue,
            'payload' => json_decode($item[1], true),
        ];
    }

    /**
     * Return the size (number of pending jobs) of the specified queue.
     *
     * @param string $queue name of the queue to be checked for pending jobs
     *
     * @return int The size of the queue.
     */
    public function size($queue)
    {
        return $this->backend->lLen(sprintf('queue:%s', $queue));
    }

    public function peekInQueue($queue, $start = 0, $count = 1)
    {
        if (1 === $count) {
            $data = json_decode($this->backend->lIndex(sprintf('queue:%s', $queue), $start), true);
            $data['queue_time'] = date_timestamp_set(date_create(), $data['queue_time']);

            return [$data];
        }

        return array_map(function ($value) {
            $data = json_decode($value, true);
            $data['queue_time'] = date_timestamp_set(date_create(), $data['queue_time']);

            return $data;
        }, $this->backend->lRange(sprintf('queue:%s', $queue), $start, $start + $count - 1));
    }

    /**
     * Create a new job and save it to the specified queue.
     *
     * @param string $queue The name of the queue to place the job in.
     * @param string $class The name of the class that contains the code to execute the job.
     * @param array $args Any optional arguments that should be passed when the job is executed.
     * @param boolean $trackStatus Set to true to be able to monitor the status of a job.
     *
     * @return string|boolean Job ID when the job was created, false if creation was cancelled due to beforeEnqueue
     */
    public function enqueue($queue, $class, $args = null, $trackStatus = false)
    {
        $id = $this->generateJobId();

        try {
            $this->dispatcher->dispatch(ResqueEvents::BEFORE_ENQUEUE, new QueueEvent($class, $args, $queue, $id));
        } catch (DontCreate $e) {
            return false;
        }

        $id = $this->createJob($queue, $class, $args, $trackStatus);

        $this->dispatcher->dispatch(ResqueEvents::AFTER_ENQUEUE, new QueueEvent($class, $args, $queue, $id));

        return $id;
    }

    /**
     * Create a new job and save it to the specified queue.
     *
     * @param string $queue The name of the queue to place the job in.
     * @param string $class The name of the class that contains the code to execute the job.
     * @param array $args Any optional arguments that should be passed when the job is executed.
     * @param boolean $monitor Set to true to be able to monitor the status of a job.
     * @param string $id Unique identifier for tracking the job. Generated if not supplied.
     * @param integer $attempts The number of attempts
     *
     * @return string
     */
    private function createJob($queue, $class, $args = null, $monitor = false, $attempts = false)
    {
        $id = self::generateJobId();

        if ($args !== null && !is_array($args)) {
            throw new \InvalidArgumentException(
                'Supplied $args must be an array.'
            );
        }

        $this->push($queue, [
            'id' => $id,
            'class' => $class,
            'args' => array($args),
            'queue_time' => microtime(true),
            'attempts' => $attempts
        ]);

        if ($monitor) {
            $this->statusManager->create($id);
        }

        return $id;
    }

    /**
     * Re-queue the current job.
     * @return string
     */
    public function recreateJob(Job $job)
    {
        return $this->createJob(
            $job->getQueue(),
            $job->getPayload()['class'],
            $job->getArguments(),
            $this->statusManager->isTracking($job->getId()),
            $job->getAttempts()
        );
    }

    /**
     * Return the status of a job.
     *
     * @return int The status of the job as one of the Status constants.
     */
    public function getJobStatus($job)
    {
        if ($job instanceof Job) {
            $job = $job->getId();
        }

        return $this->statusManager->get($job);
    }

    /**
     * @return Redis
     */
    public function getBackend()
    {
        return $this->backend;
    }

    /**
     * @return Redis
     */
    public function getSupervisor()
    {
        return $this->supervisor;
    }

    /**
     * Get worker started at
     *
     * @param string $id
     *
     * @return bool|string
     */
    public function getStartedAt($id)
    {
        return $this->backend->get(sprintf('worker:%s:started', $id));
    }

    /**
     * Get worker processed stat
     *
     * @param string $id
     *
     * @return int
     */
    public function getProcessedStat($id)
    {
        return (int) $this->backend->get(sprintf('stat:processed:%s', $id));
    }

    /**
     * Get worker failed stat
     *
     * @param string $id
     *
     * @return int
     */
    public function getFailedStat($id)
    {
        return (int) $this->backend->get(sprintf('stat:failed:%s', $id));
    }

    /**
     * Get worker heartbeat
     *
     * @param string $id
     *
     * @return int|null
     */
    public function getHeartbeat($id)
    {
        return $this->backend->get(sprintf('worker:%s:heartbeat', $id));
    }

    /**
     * Look for any workers which should be running on this server and if
     * they're not, remove them from Redis.
     */
    public function pruneDeadWorkers()
    {
        $this->supervisor->pruneDeadWorkers();
    }

    /**
     * Look for any workers which should be running on this server and if
     * they're not since heartbeat interval, remove them from Redis.
     */
    public function pruneDeadWorkersHeartbeat()
    {
        $this->supervisor->pruneDeadWorkersHeartbeat();
    }

    /**
     * Look for worker currently running
     *
     * @param $pid
     */
    public function isWorkerLive($pid)
    {
        return $this->supervisor->isWorkerLive($pid);
    }

    /**
     * Reserve and return the next available job in the specified queue.
     *
     * @param string $queue Queue to fetch next available job from.
     * @return Job|bool Instance of Resque_Job to be processed, false if none or error.
     */
    public function reserve($queue)
    {
        $payload = $this->pop($queue);

        if (!is_array($payload)) {
            return false;
        }

        return new Job($queue, $payload);
    }

    /**
     * Find the next available job from the specified queues using blocking list pop
     * and return an instance of Resque_Job for it.
     *
     * @param array             $queues
     * @param int               $timeout
     * @return bool|null|object Null when there aren't any waiting jobs, instance of Resque_Job when a job was found.
     */
    public function reserveBlocking(array $queues, $timeout = null)
    {
        $item = $this->blpop($queues, $timeout);

        if (!is_array($item)) {
            return false;
        }

        return new Job($item['queue'], $item['payload']);
    }

    /**
     * Keep worker on workers list
     *
     * @param Worker $worker
     */
    public function keepWorker(Worker $worker)
    {
        if (!$this->backend->sIsMember('workers', (string) $worker)) {
            $this->registerWorker($worker);
        }
    }

    /**
     * Register this worker in Redis.
     *
     * @param Worker $worker
     */
    public function registerWorker(Worker $worker)
    {
        $this->backend->sAdd('workers', (string)$worker);
        $this->backend->set(sprintf('worker:%s:started', (string)$worker), strftime('%Y-%m-%d %H:%M:%S'));
    }

    /**
     * Unregister this worker in Redis. (shutdown etc)
     *
     * @param Worker $worker
     */
    public function unregisterWorker(Worker $worker)
    {
        $worker->unregister();

        $id = (string)$worker;
        $this->backend->sRem('workers', $id);
        $this->backend->del(sprintf('worker:%s', $id));
        $this->backend->del(sprintf('worker:%s:started', $id));
        $this->backend->del(sprintf('worker:%s:heartbeat', $id));

        $this->stat->clear(sprintf('processed:%s', $id));
        $this->stat->clear(sprintf('failed:%s', $id));
    }

    /**
     * Insert recurring job
     *
     * @param array $job
     */
    public function insertRecurringJobs(array $data)
    {
        if (!$this->existsRecurringJobs($data['name'])) {
            $this->backend->lPush(RecurringJob::KEY_RECURRING_JOBS, json_encode($data));
        }
    }

    /**
     * Update recurring job
     *
     * @param array $data
     */
    public function updateRecurringJobs($id, array $data)
    {
        if ($this->backend->lIndex(RecurringJob::KEY_RECURRING_JOBS, $id)) {
            $this->backend->lSet(RecurringJob::KEY_RECURRING_JOBS, $id, json_encode($data));
        }
    }

    /**
     * Insert cleaner task
     *
     * @param array $data
     */
    public function insertCleanerTask(array $data)
    {
        $this->backend->lPush(Cleaner::KEY_CLEANER, json_encode($data));
    }

    /**
     * Update cleaner task
     *
     * @param $id
     * @param array $data
     */
    public function updateCleanerTask($id, array $data)
    {
        if ($this->backend->lIndex(Cleaner::KEY_CLEANER, $id)) {
            $this->backend->lSet(Cleaner::KEY_CLEANER, $id, json_encode($data));
        }
    }

    /**
     * Get cleaner task
     *
     * @param $id
     *
     * @return null|string
     */
    public function getCleanerTask($id)
    {
        return $this->backend->lIndex(Cleaner::KEY_CLEANER, $id);
    }

    /**
     * @param $id
     *
     * @return bool
     */
    public function removeFailureJob($id)
    {
        if (!$this->backend->lIndex('failed', $id)) {
            return false;
        }

        $this->backend->lSet('failed', $id, 'DELETE');
        $this->backend->lRem('failed', $id, 'DELETE');

        return true;
    }

    /**
     * Remove cleaner task
     *
     * @param $id
     */
    public function removeCleanerTask($id)
    {
        $this->backend->lSet(Cleaner::KEY_CLEANER, $id, 'DELETE');
        $this->backend->lRem(Cleaner::KEY_CLEANER, $id, 'DELETE');
    }

    /**
     * Remove recurring jobs
     *
     * @param int $id
     *
     * @return bool
     */
    public function removeRecurringJobs($id)
    {
        $job = json_decode($this->backend->lIndex(RecurringJob::KEY_RECURRING_JOBS, $id), true);

        if (!$job || !isset($job['name'])) {
            return false;
        }

        $this->removeDelayedJobs($job);

        $this->backend->del(sprintf('%s:%s', RecurringJob::KEY_HISTORY_JOBS, $job['name']));
        $this->backend->del(sprintf('%s:%s', RecurringJob::KEY_RECURRING_JOBS, $job['name']));
        $this->backend->lSet(RecurringJob::KEY_RECURRING_JOBS, $id, 'DELETE');
        $this->backend->lRem(RecurringJob::KEY_RECURRING_JOBS, $id, 'DELETE');
    }

    /**
     * Remove delayed jobs
     *
     * @param array $job
     */
    public function removeDelayedJobs($job)
    {
        if (!isset($job['name'])) {
            return false;
        }

        $items = $this->backend->keys(sprintf('delayed:*:%s', $job['name']));

        foreach ($items as $result) {
            $prefix = $this->backend->removePrefix($result);
            $result = $this->backend->lIndex($prefix, 0);
            $result = json_decode($result, true);

            $this->backend->del($prefix);

            if (!count($this->backend->keys(sprintf('delayed:%s:*', $result['timestamp'])))) {
                $this->backend->zrem('delayed_queue_schedule', $result['timestamp']);
            }
        }
    }

    /**
     * Get recurring job
     *
     * @param $id
     *
     * @return null|string
     */
    public function getRecurringJob($id)
    {
        return $this->backend->lIndex(RecurringJob::KEY_RECURRING_JOBS, $id);
    }

    public function getRecurringJobHistory($name)
    {
        return $this->backend->lRange(sprintf('%s:%s', RecurringJob::KEY_HISTORY_JOBS, $name), 0, -1);
    }

    /**
     * Check for recurring job task existence
     *
     * @return bool
     */
    public function existsRecurringJobs($name)
    {
        $jobs = $this->backend->lRange(RecurringJob::KEY_RECURRING_JOBS, 0, -1);

        foreach ($jobs as $job) {
            $job = json_decode($job, true);

            if (!isset($job['name'])) {
                continue;
            }

            if ($job['name'] === $name) {
                return true;
            }
        }

        return false;
    }

    /**
     * Check if job is already in history
     *
     * @param string $name
     * @param integer $timestamp
     *
     * @return bool
     */
    public function isJobInHistory($name, $timestamp)
    {
        $key  = sprintf('%s:%s', RecurringJob::KEY_HISTORY_JOBS, $name);
        $jobs = $this->backend->lRange($key, 0, 0);

        foreach ($jobs as $job) {
            $job = json_decode($job, true);

            if (!isset($job['args']['timestamp'])) {
                continue;
            }

            if ($job['args']['timestamp'] === $timestamp) {
                return true;
            }
        }

        return false;
    }

    /**
     * Set recurring jobs
     *
     * @param $queue
     *
     * @return mixed
     */
    public function processRecurringJobs($name)
    {
        $this->backend->set(sprintf('%s:%s', RecurringJob::KEY_RECURRING_JOBS, $name), true);
    }

    /**
     * Process recurred job cleanup.
     *
     * @param Job $job
     */
    public function performRecurringJobs(Job $job)
    {
        if (($args = $job->getArguments()) && !isset($args['name'])) {
            return false;
        }

        if (!isset($args['recurring'])) {
            return false;
        }

        $this->backend->del(sprintf('%s:%s', RecurringJob::KEY_RECURRING_JOBS, $args['name']));
    }

    /**
     * Add current recurring job in history
     *
     * @param RecurringJob $job
     * @param $runAt
     */
    public function historyRecurringJobs(RecurringJob $job, $date)
    {
        $key = sprintf('%s:%s', RecurringJob::KEY_HISTORY_JOBS, $job->getName());

        $history = new \StdClass();
        $history->start_at = $date;
        $history->name = $job->getName();
        $history->description = $job->getDescription();
        $history->args = $job->getArgs();
        $history->cron = $job->getExpression();
        $history->queue = $job->getQueue();
        $history->class = $job->getClass();
        $history->status = $job->getStatus();

        while ($this->backend->lLen($key) >= RecurringJob::HISTORY_LIMIT) {
            $this->backend->rPop($key);
        }

        if (!isset($job->getArgs()['timestamp'])) {
            return;
        }

        if (!$this->isJobInHistory($job->getName(), $job->getArgs()['timestamp'])) {
            $this->backend->lPush($key, json_encode($history));
        }
    }

    /**
     * Check for recurring jobs existence
     *
     * @param $queue
     *
     * @return mixed
     */
    public function hasRecurringJobs($name)
    {
        return $this->backend->exists(sprintf('%s:%s', RecurringJob::KEY_RECURRING_JOBS, $name));
    }

    /**
     * @param \DateTime $date
     *
     * @return int
     *
     * @throws InvalidTimestampException
     */
    public function hasDelayedJobsAt(\DateTime $date)
    {
        return $this->backend->exists(sprintf('delayed:%s', $this->getTimestamp($date)));
    }

    /**
     * @param Worker $worker
     * @param Job $job
     */
    public function updateWorkerJob(Worker $worker, Job $job = null)
    {
        if (!$job) {
            $this->backend->del('worker:' . (string)$worker);

            // Update charts
            $this->charts->getProcess()->incr();
            $this->charts->getProcess()->clean();

            $this->stat->incr('processed');
            $this->stat->incr('processed:' . (string)$worker);

            return;
        }

        $this->backend->set(sprintf('worker:%s', $worker), json_encode([
            'queue'  => $job->getQueue(),
            'run_at' => strftime('%Y-%m-%d %H:%M:%S'),
            'payload' => $job->getPayload(),
        ]));
    }

    /**
     * Get an array of all known queues.
     *
     * @return array Array of queues.
     */
    public function queues()
    {
        $queues = $this->backend->sMembers('queues');

        if (!is_array($queues)) {
            $queues = [];
        }

        return $queues;
    }

    /**
     * Remove Items from the queue
     * Safely moving each item to a temporary queue before processing it
     * If the Job matches, counts otherwise puts it in a requeue_queue
     * which at the end eventually be copied back into the original queue
     *
     * @private
     *
     * @param string $queue The name of the queue
     * @param array $items
     * @return integer number of deleted items
     */
    private function removeItems($queue, $items = array())
    {
        $counter = 0;
        $originalQueue = 'queue:'. $queue;
        $tempQueue = $originalQueue. ':temp:'. time();
        $requeueQueue = $tempQueue. ':requeue';

        // move each item from original queue to temp queue and process it
        $finished = false;
        while (!$finished) {
            $string = $this->backend->rPoplPush($originalQueue, $this->backend->getNamespace() . $tempQueue);

            if (!empty($string)) {
                if (self::matchItem($string, $items)) {
                    $this->backend->rPop($tempQueue);
                    $counter++;
                } else {
                    $this->backend->rPoplPush($tempQueue, $this->backend->getNamespace() . $requeueQueue);
                }
            } else {
                $finished = true;
            }
        }

        // move back from temp queue to original queue
        $finished = false;
        while (!$finished) {
            $string =  $this->backend->rPoplPush($requeueQueue, $this->backend->getNamespace() .$originalQueue);
            if (empty($string)) {
                $finished = true;
            }
        }

        // remove temp queue and requeue queue
        $this->backend->del($requeueQueue);
        $this->backend->del($tempQueue);

        return $counter;
    }

    public function statFailed($worker)
    {
        // Update charts
        $this->charts->getFailure()->incr();
        $this->charts->getFailure()->clean();

        $this->stat->incr('failed');
        $this->stat->incr(sprintf('failed:%s', $worker));
    }

    /**
     * Cleaner process
     */
    public function cleanerProcess()
    {
        $this->charts->getCleaned()->incr();
        $this->charts->getCleaned()->clean();
    }

    /**
     * Remove List
     *
     * @param string $queue the name of the queue
     *
     * @return int number of deleted items belongs to this list
     */
    private function removeList($queue)
    {
        $counter = self::size($queue);
        $result = $this->backend->del('queue:' . $queue);
        return ($result == 1) ? $counter : 0;
    }

    /*
     * Generate an identifier to attach to a job for status tracking.
     *
     * @return string
     */
    public static function generateJobId()
    {
        return md5(uniqid('', true));
    }

    /**
     * matching item
     * item can be ['class'] or ['class' => 'id'] or ['class' => {:foo => 1, :bar => 2}]
     * @private
     *
     * @param string $string redis result in json
     * @param $items
     *
     * @return bool
     */
    private static function matchItem($string, $items)
    {
        $decoded = json_decode($string, true);

        foreach ($items as $key => $val) {
            # class name only  ex: item[0] = ['class']
            if (is_numeric($key)) {
                if ($decoded['class'] == $val) {
                    return true;
                }
                # class name with args , example: item[0] = ['class' => {'foo' => 1, 'bar' => 2}]
            } elseif (is_array($val)) {
                $decodedArgs = (array)$decoded['args'][0];
                if ($decoded['class'] == $key &&
                    count($decodedArgs) > 0 && count(array_diff($decodedArgs, $val)) == 0) {
                    return true;
                }
                # class name with ID, example: item[0] = ['class' => 'id']
            } else {
                if ($decoded['class'] == $key && $decoded['id'] == $val) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * @return string
     */
    public static function getHostname()
    {
        if (function_exists('gethostname')) {
            return gethostname();
        }

        return php_uname('n');
    }

    /**
     * @param $id
     * @param $status
     */
    public function updateJobStatus($id, $status)
    {
        $this->statusManager->update($id, $status);
    }

    /**
     * Update recurring history job status
     *
     * @param Job $job
     * @param $status
     *
     * @return bool
     */
    public function updateRecurringJobStatus(Job $job, $status)
    {
        $args = $job->getArguments();
        $failed = $job->getFailed();

        if (!isset($args['recurring']) || !isset($args['name'])) {
            return false;
        }

        $jobs = $this->getRecurringJobHistory($args['name']);

        foreach ($jobs as $key => $job) {
            $job = json_decode($job, true);

            if (!isset($args['timestamp'])) {
                continue;
            }

            if (!isset($job['args']['timestamp'])) {
                continue;
            }

            if ($args['timestamp'] !== $job['args']['timestamp']) {
                continue;
            }

            $job['status'] = $status;

            if (null !== $failed) {
                $job['failed'] = $failed;
            }

            $this->backend->lSet(sprintf('%s:%s', RecurringJob::KEY_HISTORY_JOBS, $args['name']), $key, json_encode($job));
        }
    }

    /**
     * @param $id
     *
     * @return object
     */
    public function getService($id)
    {
        return $this->container->get($id, ContainerInterface::NULL_ON_INVALID_REFERENCE);
    }

    /**
     * Enqueue a job in a given number of seconds from now.
     *
     * Identical to Resque::enqueue, however the first argument is the number
     * of seconds before the job should be executed.
     *
     * @param int $in Number of seconds from now when the job should be executed.
     * @param string $name The name of the task
     * @param string $queue The name of the queue to place the job in.
     * @param string $class The name of the class that contains the code to execute the job.
     * @param array $args Any optional arguments that should be passed when the job is executed.
     */
    public function enqueueIn($in, $name, $queue, $class, array $args = array())
    {
        $this->enqueueAt(time() + $in, $name, $queue, $class, $args);
    }

    /**
     * Enqueue a job for execution at a given timestamp.
     *
     * Identical to Resque::enqueue, however the first argument is a timestamp
     * (either UNIX timestamp in integer format or an instance of the DateTime
     * class in PHP).
     *
     * @param \DateTime|int $at Instance of PHP DateTime object or int of UNIX timestamp.
     * @param string $name The name of the task
     * @param string $queue The name of the queue to place the job in.
     * @param string $class The name of the class that contains the code to execute the job.
     * @param array $args Any optional arguments that should be passed when the job is executed.
     */
    public function enqueueAt($at, $name, $queue, $class, $args = array())
    {
        if (empty($name)) {
            throw new DirtyExitException(sprintf('Empty name parameter for %s task', $class));
        }

        $this->validateJob($class, $queue);

        $job = $this->jobToHash($at, $name, $queue, $class, $args);

        $this->delayedPush($at, $job);

        $this->dispatcher->dispatch(ResqueEvents::AFTER_SCHEDULE, new DelayedEvent($at, $class, $args, $queue));
    }

    /**
     * Directly append an item to the delayed queue schedule.
     *
     * @param DateTime|int $timestamp Timestamp job is scheduled to be run at.
     * @param array $item Hash of item to be pushed to schedule.
     */
    public function delayedPush($timestamp, $item)
    {
        $timestamp = $this->getTimestamp($timestamp);
        $suffix = sprintf('delayed:%s:%s', $timestamp, $item['name']);

        // Handle multiple delayed jobs
        if ($this->backend->exists($suffix)) {
            return false;
        }

        $this->backend->rpush($suffix, json_encode($item));
        $this->backend->zadd('delayed_queue_schedule', $timestamp, $timestamp);
    }

    /**
     * Generate hash of all job properties to be saved in the scheduled queue.
     *
     * @param \DateTime|int $at Instance of PHP DateTime object or int of UNIX timestamp.
     * @param string $name Name of the task
     * @param string $queue Name of the queue the job will be placed on.
     * @param string $class Name of the job class.
     * @param array $args Array of job arguments.
     */

    private function jobToHash($at, $name, $queue, $class, $args)
    {
        $timestamp = $this->getTimestamp($at);

        return [
            'timestamp' => $timestamp,
            'name'  => $name,
            'class' => $class,
            'args'  => $args,
            'queue' => $queue,
        ];
    }

    /**
     * If there are no jobs for a given key/timestamp, delete references to it.
     *
     * Used internally to remove empty delayed: items in Redis when there are
     * no more jobs left to run at that timestamp.
     *
     * @param string $key Key to count number of items at.
     * @param int $timestamp Matching timestamp for $key.
     */
    private function cleanupTimestamp($key, $timestamp)
    {
        $key  = $this->backend->removePrefix($key);
        $item = $this->backend->lpop($key);
        $item = json_decode($item, true);

        $timestamp = $this->getTimestamp($timestamp);

        if ($this->backend->llen($key) == 0) {
            $this->backend->del($key);
            $this->backend->zrem('delayed_queue_schedule', $timestamp);
        }

        return $item;
    }

    /**
     * Convert a timestamp in some format in to a unix timestamp as an integer.
     *
     * @param DateTime|int $timestamp Instance of DateTime or UNIX timestamp.
     * @return int Timestamp
     * @throws InvalidTimestampException
     */
    private function getTimestamp($timestamp)
    {
        if ($timestamp instanceof \DateTime) {
            $timestamp = $timestamp->getTimestamp();
        }

        if ((int)$timestamp != $timestamp) {
            throw new InvalidTimestampException(
                'The supplied timestamp value could not be converted to an integer.'
            );
        }

        return (int) $timestamp;
    }

    /**
     * Find the first timestamp in the delayed schedule before/including the timestamp.
     *
     * Will find and return the first timestamp upto and including the given
     * timestamp. This is the heart of the ResqueScheduler that will make sure
     * that any jobs scheduled for the past when the worker wasn't running are
     * also queued up.
     *
     * @param DateTime|int $timestamp Instance of DateTime or UNIX timestamp.
     *                                Defaults to now.
     * @return int|false UNIX timestamp, or false if nothing to run.
     */
    public function nextDelayedTimestamp($at = null)
    {
        if ($at === null) {
            $at = time();
        } else {
            $at = self::getTimestamp($at);
        }

        $items = $this->backend->zrangebyscore('delayed_queue_schedule', '-inf', $at, array('limit' => array(0, 1)));

        if (!empty($items)) {
            return $items[0];
        }

        return false;
    }

    /**
     * Pop a job off the delayed queue for a given timestamp.
     *
     * @param DateTime|int $timestamp Instance of DateTime or UNIX timestamp.
     * @return array Matching job at timestamp.
     */
    public function nextItemForTimestamp($timestamp)
    {
        $items = $this->backend->keys(sprintf('delayed:%s:*', $timestamp));

        foreach ($items as $key) {
            return $this->cleanupTimestamp($key, $timestamp);
        }

        return false;
    }

    /**
     * Ensure that supplied job class/queue is valid.
     *
     * @param string $class Name of job class.
     * @param string $queue Name of queue.
     * @throws Resque_Exception
     */
    private function validateJob($class, $queue)
    {
        if (empty($class)) {
            throw new Exception('Jobs must be given a class.');
        } else if (empty($queue)) {
            throw new Exception('Jobs must be put in a queue.');
        }

        return true;
    }

    /**
     * @return array
     */
    public function getInfos()
    {
        return [
            ':queues' => $this->backend->sCard('queues'),
            ':workers' => $this->backend->sCard('workers'),
            ':failed' => $this->backend->lLen('failed'),
            ':recurring' => $this->backend->lLen('recurring'),
            ':delayed' => $this->backend->zCard('delayed_queue_schedule'),
            ':stat:processed' => $this->backend->get('stat:processed'),
            ':stat:failed' => $this->backend->get('stat:failed'),
        ];
    }
}
