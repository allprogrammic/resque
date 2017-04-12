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

class JobFailEvent extends JobEvent
{
    private $exception;

    /**
     * JobEvent constructor.
     *
     * @param $job
     * @param $exception
     */
    public function __construct($job, $exception)
    {
        parent::__construct($job);

        $this->exception = $exception;
    }

    /**
     * @return \Exception
     */
    public function getException()
    {
        return $this->exception;
    }
}
