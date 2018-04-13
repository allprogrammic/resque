<?php

/*
 * This file is part of the AllProgrammic Resque package.
 *
 * (c) AllProgrammic SAS <contact@allprogrammic.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace AllProgrammic\Component\Resque\Recurring;

use AllProgrammic\Component\Resque\RecurringJob;

/**
 * Redis backend for storing recurring Resque jobs.
 */
class Redis implements RecurringInterface
{
    private $backend;

    public function __construct(\AllProgrammic\Component\Resque\Redis $backend)
    {
        $this->backend = $backend;
    }

    /**
     * Count number of items in the recurred queue
     *
     * @return int
     */
    public function count()
    {
        return $this->backend->lLen(RecurringJob::KEY_RECURRING_JOBS);
    }

    /**
     * @param int $start
     * @param int $count
     *
     * @return array|mixed
     */
    public function peek($start = 0, $count = 1)
    {
        if (1 === $count) {
            return json_decode($this->backend->lIndex(RecurringJob::KEY_RECURRING_JOBS, $start), true);
        }

        return array_map(function ($value) {
            return json_decode($value, true);
        }, $this->backend->lRange(RecurringJob::KEY_RECURRING_JOBS, $start, $start + $count - 1));
    }
}
