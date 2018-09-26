<?php

/*
 * This file is part of the AllProgrammic Resque package.
 *
 * (c) AllProgrammic SAS <contact@allprogrammic.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace AllProgrammic\Component\Resque\Cleaner;

use AllProgrammic\Component\Resque\Worker;

/**
 * Interface that all cleaner backends should implement.
 */
interface CleanerInterface
{
    public function count();

    public function peek($start = 0, $count = 1);
}
