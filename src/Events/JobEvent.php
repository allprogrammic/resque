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

class JobEvent extends Event
{
    private $job;

    /**
     * JobEvent constructor.
     *
     * @param $job
     */
    public function __construct($job)
    {
        $this->job = $job;
    }

    /**
     * @return mixed
     */
    public function getJob()
    {
        return $this->job;
    }
}
