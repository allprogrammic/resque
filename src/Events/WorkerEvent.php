<?php

/*
 * This file is part of the AllProgrammic Resque package.
 *
 * (c) AllProgrammic SAS <contact@allprogrammic.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace AllProgrammic\Component\Resque\Events;

use AllProgrammic\Component\Resque\Worker;
use Symfony\Component\EventDispatcher\Event;

class WorkerEvent extends Event
{
    private $worker;

    /**
     * WorkerEvent constructor.
     *
     * @param $worker
     */
    public function __construct($worker)
    {
        $this->worker = $worker;
    }

    /**
     * @return Worker
     */
    public function getWorker()
    {
        return $this->worker;
    }
}
