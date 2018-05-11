<?php

/*
 * This file is part of the AllProgrammic Resque package.
 *
 * (c) AllProgrammic SAS <contact@allprogrammic.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace AllProgrammic\Component\Resque\Charts\Failure;

use AllProgrammic\Component\Resque\Charts\Charts;
use AllProgrammic\Component\Resque\Charts\ChartsInterface;
use AllProgrammic\Component\Resque\Worker;

/**
 * Class Redis
 *
 * @package AllProgrammic\Component\Resque\Charts\Failure
 */
class Redis implements ChartsInterface
{
    /**
     * @var \AllProgrammic\Component\Resque\Redis
     */
    private $backend;

    /**
     * Redis constructor.
     *
     * @param \AllProgrammic\Component\Resque\Redis $backend
     */
    public function __construct(\AllProgrammic\Component\Resque\Redis $backend)
    {
        $this->backend = $backend;
    }

    /**
     * @param int $interval
     */
    public function clean($interval = 7)
    {
        $data = $this->peek(0, 0);
        $date = date('Y-m-d', strtotime(sprintf('-%s days', $interval)));
        $date = strtotime($date);

        foreach ($data as $value) {
            if ($value['date'] < $date) {
                $this->backend->del(sprintf('%s:%s', Charts::INDEX_FAILURE, $value['date']));
            }
        }
    }

    /**
     * @return int
     */
    public function incr()
    {
        return $this->backend->incrBy(sprintf('%s:%s', Charts::INDEX_FAILURE, strtotime(date('Y-m-d'))), 1);
    }

    /**
     * @param int $start
     * @param int $count
     *
     * @return array
     */
    public function peek($start = 0, $count = 1)
    {
        $keys = $this->backend->keys(sprintf('%s:*', Charts::INDEX_FAILURE));
        $data = [];

        foreach ($keys as $result) {
            $time = substr($result, strlen(sprintf('%s%s:', $this->backend->getNamespace(), Charts::INDEX_FAILURE)));

            $data[] = [
                'date'  => (int) $time,
                'count' => (int) $this->backend->get(sprintf('%s:%s', Charts::INDEX_FAILURE, $time)),
            ];
        }

        return $data;
    }
}
