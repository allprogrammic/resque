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
    private $key;

    /** @var string */
    const LOCK_KEY = 'lock:%s';

    /** @var int */
    const LOCK_INTERVAL = 30;

    /**
     * Lock constructor.
     *
     * @param Redis $backend
     * @param $key
     */
    public function __construct(Redis $backend, $key) {
        $this->backend = $backend;
        $this->key = sprintf(self::LOCK_KEY, $key);
    }

    /**
     * Reserve lock
     *
     * @param $key
     *
     * @return bool
     */
    public function lock()
    {
        $now = time();
        $timeout = $now + self::LOCK_INTERVAL + 1;

        if ($this->backend->setNx($this->key, $timeout)) {
            return true;
        }

        if ($now <= $this->backend->get($this->key)) {
            return false;
        }

        return $now > $this->backend->getSet($this->key, $timeout);
    }

    /**
     * Perform lock
     *
     * @param $key
     */
    public function release()
    {
        $this->backend->del($this->key);
    }
}