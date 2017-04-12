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

class QueueEvent extends Event
{
    protected $class;

    protected $args;

    protected $queue;

    protected $id;

    /**
     * QueueEvent constructor.
     *
     * @param $class
     * @param $args
     * @param $queue
     * @param $id
     */
    public function __construct($class, $args, $queue, $id)
    {
        $this->class = $class;
        $this->args = $args;
        $this->queue = $queue;
        $this->id = $id;
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

    /**
     * @return mixed
     */
    public function getId()
    {
        return $this->id;
    }
}
