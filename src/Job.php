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

use AllProgrammic\Component\Resque\Failure\FailureInterface;
use AllProgrammic\Component\Resque\Job\DontPerform;

/**
 * Resque job
 */
class Job
{
    /**
     * @var string The name of the queue that this job belongs to.
     */
    private $queue;

    /**
     * @var array Array containing details of the job.
     */
    private $payload;

    /**
     * @var Worker Instance of the Resque worker running this job.
     */
    private $worker;

    /**
     * @var object Instance of the class performing work for this job.
     */
    private $instance;

    /**
     * @var integer
     */
    private $failed;

    /**
     * @var integer
     */
    private $attempts;

    /**
     * Job constructor.
     *
     * @param $queue
     * @param $payload
     * @param null $queue
     */
    public function __construct($queue, $payload, $worker = null)
    {
        $this->queue   = $queue;
        $this->payload = $payload;

        if (null !== $worker) {
            $this->worker = $worker;
        }
    }

    /**
     * Update the status of the current job.
     *
     * @param int $status Status constant from Resque_Job_Status indicating the current status of a job.
     */
    public function updateStatus($status)
    {
        if (empty($this->payload['id'])) {
            return;
        }

        $this->worker->updateJobStatus($this, $status);
    }

    /**
     * Get the arguments supplied to this job.
     *
     * @return array Array of arguments.
     */
    public function getId()
    {
        if (!isset($this->payload['id'])) {
            return null;
        }

        return $this->payload['id'];
    }

    /**
     * Get the arguments supplied to this job.
     *
     * @return array Array of arguments.
     */
    public function getArguments()
    {
        if (!isset($this->payload['args'])) {
            return array();
        }

        return $this->payload['args'][0];
    }

    /**
     * Get the instantiated object for this job that will be performing work.
     *
     * @return object Instance of the object that this job belongs to.
     * @throws Exception
     */
    public function getInstance()
    {
        if (!is_null($this->instance)) {
            return $this->instance;
        }

        $this->instance = $this->worker->getService($this->payload['class']);

        if (!$this->instance) {
            throw new Exception(
                'Could not find job service ' . $this->payload['class'] . '.'
            );
        }

        if (!$this->instance instanceof AbstractJob) {
            throw new Exception(
                sprintf('Job service %s does inherit from AbstractJob class.', $this->payload['class'])
            );
        }

        $this->instance->setJob($this);
        $this->instance->setQueue($this->queue);

        return $this->instance;
    }

    /**
     * Actually execute a job by calling the perform method on the class
     * associated with the job with the supplied arguments.
     *
     * @return bool
     * @throws Exception When the job's class could not be found or it does not contain a perform method.
     */
    public function perform()
    {
        $this->getInstance()->perform($this->getArguments());

        return true;
    }

    /**
     * Mark the current job as having failed.
     *
     * @param FailureInterface $failureHandler
     * @param $exception
     */
    public function fail(FailureInterface $failureHandler, $exception)
    {
        return $failureHandler->onFail(
            $this->payload,
            $exception,
            $this->worker,
            $this->queue
        );
    }

    /**
     * @return string
     */
    public function getQueue()
    {
        return $this->queue;
    }

    /**
     * @param string $queue
     */
    public function setQueue(string $queue)
    {
        $this->queue = $queue;
    }

    /**
     * @return array
     */
    public function getPayload()
    {
        return $this->payload;
    }

    /**
     * @param array $payload
     */
    public function setPayload(array $payload)
    {
        $this->payload = $payload;
    }

    /**
     * @return Worker
     */
    public function getWorker()
    {
        return $this->worker;
    }

    /**
     * @param Worker $worker
     */
    public function setWorker(Worker $worker)
    {
        $this->worker = $worker;
    }

    /**
     * @return int
     */
    public function getFailed()
    {
        return $this->failed;
    }

    /**
     * @param int $failed
     */
    public function setFailed(int $failed)
    {
        $this->failed = $failed;
    }

    /**
     * @return int
     */
    public function getAttempts()
    {
        return $this->attempts;
    }

    /**
     * @param int $attempts
     */
    public function setAttempts(int $attempts)
    {
        $this->attempts = $attempts;
    }

    /**
     * Generate a string representation used to describe the current job.
     *
     * @return string The string representation of the job.
     */
    public function __toString()
    {
        $name = [sprintf('Job{%s}', $this->queue)];

        if (!empty($this->payload['id'])) {
            $name[] = sprintf('ID: %s', $this->payload['id']);
        }

        $name[] = $this->payload['class'];

        if (!empty($this->payload['args'])) {
            $name[] = json_encode($this->payload['args']);
        }

        return sprintf('(%s)', implode(' | ', $name));
    }
}
