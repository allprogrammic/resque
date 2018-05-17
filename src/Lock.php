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

use AllProgrammic\Component\Resque\Job\InvalidRecurringJobException;

class Lock
{
    /** @var string */
    const LOCK_KEY = 'lock:delayed';

    /** @var int */
    const LOCK_INTERVAL = 10;


    public function __construct(Redis $backend) {
        $this->backend = $backend;
    }

    public function getLock($args)
    {
        if (!isset($args['name'])) {
            throw new InvalidRecurringJobException();
        }

        if (!isset($args['timestamp'])) {
            throw new InvalidRecurringJobException();
        }

        return sprintf('%s:%s:%s', self::LOCK_KEY, $args['name'], $args['timestamp']);
    }

    public function enqueueLock($args)
    {
        $key = $this->getLock($args);
        $now = time();
        $timeout = $now + self::LOCK_INTERVAL + 1;

        if ($this->backend->setNx($key, $timeout)) {
            return true;
        }

        if ($now <= $this->backend->get($key)) {
            return false;
        }

        return $now > $this->backend->getSet($key, $timeout);
    }

    public function performLock($args)
    {
        $this->backend->del($this->getLock($args));
    }
}