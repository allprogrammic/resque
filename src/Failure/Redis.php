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
    public function onFail($payload, \Exception $exception, Worker $worker, $queue)
    {
        $data = new \stdClass;
        $data->failed_at = strftime('%a %b %d %H:%M:%S %Z %Y');
        $data->payload = $payload;
        $data->exception = get_class($exception);
        $data->error = $exception->getMessage();
        $data->backtrace = explode("\n", $exception->getTraceAsString());
        $data->worker = (string)$worker;
        $data->queue = $queue;

        $this->backend->rPush('failed', json_encode($data));
    }
}
