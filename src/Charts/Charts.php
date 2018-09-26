<?php

/*
 * This file is part of the AllProgrammic Resque package.
 *
 * (c) AllProgrammic SAS <contact@allprogrammic.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace AllProgrammic\Component\Resque\Charts;

use AllProgrammic\Component\Resque\Worker;

/**
 * Class Charts
 *
 * @package AllProgrammic\Component\Resque\Charts\Processed
 */
class Charts
{
    const INDEX_FAILURE = 'charts:failure';
    const INDEX_PROCESS = 'charts:process';
    const INDEX_CLEANED = 'charts:cleaned';

    /**
     * Redis constructor.
     *
     * @param \AllProgrammic\Component\Resque\Redis $backend
     */
    public function __construct(\AllProgrammic\Component\Resque\Redis $backend)
    {
        $this->backend = $backend;
    }

    public function getFailure()
    {
        return new \AllProgrammic\Component\Resque\Charts\Failure\Redis($this->backend);
    }

    public function getProcess()
    {
        return new \AllProgrammic\Component\Resque\Charts\Process\Redis($this->backend);
    }

    public function getCleaned()
    {
        return new \AllProgrammic\Component\Resque\Charts\Cleaner\Redis($this->backend);
    }
}