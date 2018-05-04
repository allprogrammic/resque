<?php

/*
 * This file is part of the AllProgrammic Resque package.
 *
 * (c) AllProgrammic SAS <contact@allprogrammic.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace AllProgrammic\Component\Resque\Failure;

use AllProgrammic\Component\Resque\Worker;

/**
 * Redis backend for storing failed Resque jobs.
 */
class Redis implements FailureInterface
{
    private $backend;

    public function __construct(\AllProgrammic\Component\Resque\Redis $backend)
    {
        $this->backend = $backend;
    }

    /**
     * Initialize a failed job class and save it (where appropriate).
     *
     * @param array $payload Object containing details of the failed job.
     * @param \Exception $exception Instance of the exception that was thrown by the failed job.
     * @param Worker $worker Instance of Resque_Worker that received the job.
     * @param string $queue The name of the queue the job was fetched from.
     */
    public function onFail($payload, $exception, Worker $worker, $queue)
    {
        $data = new \stdClass;
        $data->failed_at = new \DateTime();
        $data->payload = $payload;
        $data->exception = get_class($exception);
        $data->error = $exception->getMessage();
        $data->backtrace = explode("\n", $exception->getTraceAsString());
        $data->worker = (string)$worker;
        $data->queue = $queue;

        return $this->backend->lPush('failed', json_encode($data));
    }

    /**
     * Count number of items in the failed queue
     *
     * @return int
     */
    public function count()
    {
        return $this->backend->lLen('failed');
    }

    public function peek($start = 0, $count = 1)
    {
        if (1 === $count) {
            $data = json_decode($this->backend->lIndex('failed', $start), true);
            return [$data];
        }

        return array_map(function ($value) {
            $data = json_decode($value, true);
            return $data;
        }, $this->backend->lRange('failed', $start, $start + $count - 1));
    }
}
