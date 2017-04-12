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
 * Interface that all failure backends should implement.
 */
interface FailureInterface
{
    /**
     * Initialize a failed job class and save it (where appropriate).
     *
     * @param array $payload Object containing details of the failed job.
     * @param \Exception $exception Instance of the exception that was thrown by the failed job.
     * @param Worker $worker Instance of Resque_Worker that received the job.
     * @param string $queue The name of the queue the job was fetched from.
     */
    public function onFail($payload, \Exception $exception, Worker $worker, $queue);

    public function count();

    public function peek($start = 0, $count = 1);
}
