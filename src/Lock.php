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
    private $prefix;

    /** @var string */
    const LOCK_KEY = 'lock:%s';

    /** @var int */
    const LOCK_INTERVAL = 10;

    /**
     * Lock constructor.
     *
     * @param Redis $backend
     * @param $prefix
     */
    public function __construct(Redis $backend, $prefix) {
        $this->backend = $backend;
        $this->prefix  = $prefix;
    }

    public function getLock($args)
    {
        if (!isset($args['name'])) {
            throw new InvalidRecurringJobException();
        }

        if (!isset($args['timestamp'])) {
            throw new InvalidRecurringJobException();
        }

        return sprintf('%s:%s:%s', sprintf(self::LOCK_KEY, $this->prefix), $args['name'], $args['timestamp']);
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