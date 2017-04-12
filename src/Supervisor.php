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

use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;
use AllProgrammic\Component\Resque\Failure\FailureInterface;
use Symfony\Component\EventDispatcher\EventDispatcherInterface;

class Supervisor
{
    /** @var Engine */
    private $engine;

    /** @var Redis */
    private $backend;

    /** @var EventDispatcherInterface */
    private $dispatcher;

    /** @var FailureInterface */
    private $failureHandler;

    /** @var string */
    private $hostname;

    /** @var LoggerInterface */
    private $logger;

    public function __construct(
        Engine $engine,
        Redis $backend,
        EventDispatcherInterface $dispatcher,
        FailureInterface $failureHandler,
        LoggerInterface $logger = null
    ) {
        $this->engine = $engine;
        $this->backend = $backend;
        $this->dispatcher = $dispatcher;
        $this->failureHandler = $failureHandler;
        $this->hostname = Engine::getHostname();
        $this->logger = $logger;
    }

    /**
     * Return all workers known to Resque as instantiated instances.
     * @return array
     */
    public function all()
    {
        if (!is_array($workers = $this->backend->sMembers('workers'))) {
            $workers = [];
        }

        $instances = [];
        foreach ($workers as $workerId) {
            $instances[] = $this->find($workerId);
        }

        return $instances;
    }

    /**
     * Given a worker ID, find it and return an instantiated worker class for it.
     *
     * @param string $workerId The ID of the worker.
     *
     * @return Worker|bool Instance of the worker. False if the worker does not exist.
     */
    public function find($workerId)
    {
        if (!$this->exists($workerId) || false === strpos($workerId, ":")) {
            return false;
        }

        list($hostname, $pid, $queues) = explode(':', $workerId, 3);

        $worker = new Worker(
            $this->engine,
            $this->dispatcher,
            $this->failureHandler,
            explode(',', $queues),
            $this->logger
        );
        $worker->setId($workerId);

        return $worker;
    }

    /**
     * Look for any workers which should be running on this server and if
     * they're not, remove them from Redis.
     *
     * This is a form of garbage collection to handle cases where the
     * server may have been killed and the Resque workers did not die gracefully
     * and therefore leave state information in Redis.
     */
    public function pruneDeadWorkers()
    {
        $workerPids = $this->workerPids();
        $workers = $this->all();

        foreach ($workers as $worker) {
            if (is_object($worker)) {
                list($host, $pid, $queues) = explode(':', (string)$worker, 3);
                if ($host != $this->hostname || in_array($pid, $workerPids) || $pid == getmypid()) {
                    continue;
                }

                if ($this->logger) {
                    $this->logger->log(LogLevel::INFO, sprintf('Pruning dead worker: %s', (string)$worker));
                }

                $this->engine->unregisterWorker($worker);
            }
        }
    }

    /**
     * Given a worker ID, check if it is registered/valid.
     *
     * @param string $workerId ID of the worker.
     * @return boolean True if the worker exists, false if not.
     */
    public function exists($workerId)
    {
        return (bool)$this->backend->sIsMember('workers', $workerId);
    }

    /**
     * Return an array of process IDs for all of the Resque workers currently
     * running on this machine.
     *
     * @return array Array of Resque worker process IDs.
     */
    public function workerPids()
    {
        $pids = [];
        exec('ps -A -o pid,command | grep [r]esque', $cmdOutput);

        foreach ($cmdOutput as $line) {
            list($pids[], ) = explode(' ', trim($line), 2);
        }

        return $pids;
    }
}
