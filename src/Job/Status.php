<?php

/*
 * This file is part of the AllProgrammic Resque package.
 *
 * (c) AllProgrammic SAS <contact@allprogrammic.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */


namespace AllProgrammic\Component\Resque\Job;

use AllProgrammic\Component\Resque\Redis;

/**
 * Status tracker/information for a job.
 */
class Status
{
    const STATUS_WAITING = 1;
    const STATUS_RUNNING = 2;
    const STATUS_FAILED = 3;
    const STATUS_COMPLETE = 4;

    /** @var Redis */
    private $backend;

    /**
     * @var array Array of statuses that are considered final/complete.
     */
    private static $completeStatuses = array(
        self::STATUS_FAILED,
        self::STATUS_COMPLETE
    );

    /**
     * Setup a new instance of the job monitor class for the supplied job ID.
     *
     * @param string $id The ID of the job to manage the status for.
     */
    public function __construct(Redis $backend)
    {
        $this->backend = $backend;
    }

    /**
     * Create a new status monitor item for the supplied job ID. Will create
     * all necessary keys in Redis to monitor the status of a job.
     *
     * @param string $id The ID of the job to monitor the status of.
     */
    public function create($id)
    {
        $statusPacket = array(
            'status' => self::STATUS_WAITING,
            'updated' => time(),
            'started' => time(),
        );
        $this->backend->set($this->getKey($id), json_encode($statusPacket));
    }

    /**
     * Check if we're actually checking the status of the loaded job status
     * instance.
     *
     * @param $id
     *
     * @return bool True if the status is being monitored, false if not.
     */
    public function isTracking($id)
    {
        if (!$this->backend->exists($this->getKey($id))) {
            return false;
        }

        return true;
    }

    /**
     * Update the status indicator for the current job with a new status.
     *
     * @param $id
     * @param int $status The status of the job (see constants in Status)
     */
    public function update($id, $status)
    {
        if (!$this->isTracking($id)) {
            return;
        }

        $this->backend->set($this->getKey($id), json_encode([
            'status' => $status,
            'updated' => time(),
        ]));

        // Expire the status for completed jobs after 24 hours
        if (in_array($status, self::$completeStatuses)) {
            $this->backend->expire($this->getKey($id), 86400);
        }
    }

    /**
     * Fetch the status for the job being monitored.
     *
     * @param $id
     *
     * @return mixed False if the status is not being monitored, otherwise the status as
     *    as an integer, based on the Status constants.
     */
    public function get($id)
    {
        if (!$this->isTracking($id)) {
            return false;
        }

        $statusPacket = json_decode($this->backend->get($this->getKey($id)), true);

        if (!$statusPacket) {
            return false;
        }

        return $statusPacket['status'];
    }

    /**
     * Stop tracking the status of a job.
     *
     * @param $id
     */
    public function stop($id)
    {
        $this->backend->del($this->getKey($id));
    }

    /**
     * Generate a string representation of this object.
     *
     * @param $id
     *
     * @return string String representation of the current job status class.
     */
    private function getKey($id)
    {
        return sprintf('job:%s:status', $id);
    }
}
