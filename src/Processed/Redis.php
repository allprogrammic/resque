<?php

/*
 * This file is part of the AllProgrammic Resque package.
 *
 * (c) AllProgrammic SAS <contact@allprogrammic.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace AllProgrammic\Component\Resque\Processed;

use AllProgrammic\Component\Resque\Worker;

/**
 * Redis backend for storing failed Resque jobs.
 */
class Redis implements ProcessedInterface
{
    private $backend;

    public function __construct(\AllProgrammic\Component\Resque\Redis $backend)
    {
        $this->backend = $backend;
    }

    public function peek($start = 0, $count = 1)
    {
        $keys = $this->backend->keys('processed:*');
        $data = [];

        foreach ($keys as $result) {
            $time = substr($result, strlen($this->backend->getNamespace() . 'processed:'));

            $data[] = [
                'processed_at' => (int) $time,
                'count' => (int) $this->backend->get(sprintf('processed:%s', $time)),
            ];
        }

        return $data;
    }
}
