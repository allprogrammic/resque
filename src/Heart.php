<?php

/*
 * This file is part of the AllProgrammic Resque package.
 *
 * (c) AllProgrammic SAS <contact@allprogrammic.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */


namespace AllProgrammic\Component\Resque;

class Heart
{
    /** @var Redis */
    private $backend;

    /** @var string */
    const HEARTBEAT_KEY = 'worker:%s:%s:tasks:heartbeat';

    /** @var int */
    const HEARTBEAT_INTERVAL = 60;

    /**
     * Engine constructor.
     *
     * @param Redis $backend
     */
    public function __construct(Redis $backend)
    {
        $this->backend = $backend;
    }

    /**
     * Beat this worker in Redis
     *
     * @param Worker $worker
     */
    public function beat(Worker $worker)
    {
        $this->backend->set(sprintf('worker:%s:heartbeat', (string)$worker), time());
    }
}
