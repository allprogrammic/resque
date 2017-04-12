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

class ResqueEvents
{
    const BEFORE_ENQUEUE = 'resque.beforeEnqueue';

    const AFTER_ENQUEUE = 'resque.afterEnqueue';

    const BEFORE_FORK = 'resque.beforeFork';

    const AFTER_FORK = 'resque.afterFork';

    const BEFORE_FIRST_FORK = 'resque.beforeFirstFork';

    const JOB_BEFORE_PERFORM = 'resque.beforePerform';

    const JOB_AFTER_PERFORM = 'resque.afterPerform';

    const JOB_FAILURE = 'resque.onJobFailure';
}
