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

class Manager implements FailureInterface
{
    /**
     * @var FailureInterface
     */
    private $backend;

    public function __construct(FailureInterface $bakend)
    {
        $this->backend = $bakend;
    }

    /**
     * Create a new failed job on the backend.
     *
     * @param array $payload        The contents of the job that has just failed.
     * @param \Exception $exception  The exception generated when the job failed to run.
     * @param Worker $worker Instance of Resque_Worker that was running this job when it failed.
     * @param string $queue          The name of the queue that this job was fetched from.
     */
    public function onFail($payload, \Exception $exception, Worker $worker, $queue)
    {
        $this->backend->onFail($payload, $exception, $worker, $queue);
    }
}
