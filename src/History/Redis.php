<?php

/*
 * This file is part of the AllProgrammic Resque package.
 *
 * (c) AllProgrammic SAS <contact@allprogrammic.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace AllProgrammic\Component\Resque\History;

use AllProgrammic\Component\Resque\RecurringJob;

/**
 * Redis backend for storing recurring history Resque jobs.
 */
class Redis implements HistoryInterface
{
    private $backend;

    /** @var string */
    private $name;

    public function __construct(\AllProgrammic\Component\Resque\Redis $backend)
    {
        $this->backend = $backend;
    }

    /**
     * @return string
     */
    public function getName()
    {
        return $this->name;
    }

    /**
     * @param string $name
     */
    public function setName($name)
    {
        $this->name = $name;
    }

    /**
     * Count number of items in the recurred history queue
     *
     * @return int
     */
    public function count()
    {
        return $this->backend->lLen(sprintf('%s:%s', RecurringJob::KEY_HISTORY_JOBS, $this->name));
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
            return json_decode($this->backend->lIndex(sprintf('%s:%s', RecurringJob::KEY_HISTORY_JOBS, $this->name), $start), true);
        }

        return array_map(function ($value) {
            return json_decode($value, true);
        }, $this->backend->lRange(sprintf('%s:%s', RecurringJob::KEY_HISTORY_JOBS, $this->name), $start, $start + $count - 1));
    }
}
