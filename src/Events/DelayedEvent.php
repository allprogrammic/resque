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

use Symfony\Component\EventDispatcher\Event;

class DelayedEvent extends Event
{
    protected $at;

    protected $class;

    protected $args;

    protected $queue;

    /**
     * QueueEvent constructor.
     *
     * @param $class
     * @param $args
     * @param $queue
     * @param $id
     */
    public function __construct($at, $class, $args, $queue)
    {
        $this->at = $at;
        $this->class = $class;
        $this->args = $args;
        $this->queue = $queue;
    }

    /**
     * @return mixed
     */
    public function getAt()
    {
        return $this->at;
    }

    /**
     * @return mixed
     */
    public function getClass()
    {
        return $this->class;
    }

    /**
     * @return mixed
     */
    public function getArgs()
    {
        return $this->args;
    }

    /**
     * @return mixed
     */
    public function getQueue()
    {
        return $this->queue;
    }
}
