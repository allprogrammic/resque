<?php

/*
 * This file is part of the AllProgrammic Resque package.
 *
 * (c) AllProgrammic SAS <contact@allprogrammic.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace AllProgrammic\Component\Resque\Cleaner;

use AllProgrammic\Component\Resque\Cleaner;

/**
 * Redis backend for storing recurring Resque jobs.
 */
class Redis implements CleanerInterface
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
        return $this->backend->lLen(Cleaner::KEY_CLEANER);
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
            return json_decode($this->backend->lIndex(Cleaner::KEY_CLEANER, $start), true);
        }

        return array_map(function ($value) {
            return json_decode($value, true);
        }, $this->backend->lRange(Cleaner::KEY_CLEANER, $start, $start + $count - 1));
    }
}
