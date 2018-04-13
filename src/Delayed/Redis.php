<?php

/*
 * This file is part of the AllProgrammic Resque package.
 *
 * (c) AllProgrammic SAS <contact@allprogrammic.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace AllProgrammic\Component\Resque\Delayed;

use AllProgrammic\Component\Resque\Worker;

/**
 * Redis backend for storing delayed Resque jobs.
 */
class Redis implements DelayedInterface
{
    private $backend;

    public function __construct(\AllProgrammic\Component\Resque\Redis $backend)
    {
        $this->backend = $backend;
    }

    /**
     * Count number of items in the delayed queue
     *
     * @return int
     */
    public function count()
    {
        return $this->backend->zCard('delayed_queue_schedule');
    }

    /**
     * @param int $start
     * @param int $count
     *
     * @return array
     */
    public function peek($start = 0, $count = 1)
    {
        return array_map(function ($value) {
            $data = $this->backend->lIndex($this->backend->removePrefix($value), 0);
            $data = json_decode($data, true);
            $data['start_at'] = new \DateTime(date('Y-m-d H:i:s', $data['timestamp']));

            return $data;
        }, $this->backend->keys('delayed:*'));
    }
}
