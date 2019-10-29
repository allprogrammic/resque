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

class Cleaner
{
    const KEY_CLEANER = 'cleaner';

    /**
     * @var Engine
     */
    private $engine;

    /**
     * @var array
     */
    private $task;

    /**
     * Cleaner constructor.
     *
     * @param Engine $engine
     * @param $task
     */
    public function __construct(Engine $engine, $task)
    {
        $this->engine = $engine;
        $this->task = $task;
    }

    /**
     * @return Engine
     */
    public function getEngine()
    {
        return $this->engine;
    }

    /**
     * @param Engine $engine
     */
    public function setEngine(Engine $engine)
    {
        $this->engine = $engine;
    }

    /**
     * @return array
     */
    public function getTask()
    {
        return $this->task;
    }

    /**
     * @param array $task
     */
    public function setTask(array $task)
    {
        $this->task = $task;
    }

    /**
     * @param $failure
     *
     * @return bool
     */
    public function canCatchException($failure)
    {
        if (!isset($failure['exception'])) {
            return false;
        }

        if (empty($this->task['exception'])) {
            return true;
        }

        if (!preg_match(sprintf('/%s/i', $this->task['exception']), $failure['exception'])) {
            return false;
        }

        return true;
    }

    /**
     * Handle cleaner tasks
     */
    public function handle()
    {
        $offset = 0;
        $items  = $this->next($offset);

        while (!empty($items)) {
            $result = $items[0];

            if (!$this->canCatchException($result)) {
                $items = $this->next($offset++);
                continue;
            }

            if (!isset($result['queue'])) {
                $items = $this->next($offset++);
                continue;
            }

            if (!isset($result['payload'])) {
                $items = $this->next($offset++);
                continue;
            }

            $payload  = $result['payload'];
            $attempts = 1;

            // Skip different queue
            if (!empty($this->task['queue']) && $this->task['queue'] !== $result['queue']) {
                $items = $this->next($offset++);
                continue;
            }

            if (isset($payload['attempts'])) {
                $attempts = $payload['attempts'] + 1;
            }

            /** @var $alert bool */
            $alert = $this->task['alert'];

            if ($attempts > $this->task['attempts'] || !$this->engine->removeFailureJob($offset)) {
                // Handle mail alert
                if (isset($alert) && $alert) {
                    $this->engine->sendMail($payload, 'failure');
                }

                $items = $this->next($offset++);
                continue;
            }

            $job = new Job($result['queue'], $result['payload']);
            $job->setAttempts($attempts);

            $this->engine->recreateJob($job);
            $items = $this->next($offset++);

            // Cleaner process to update graphs
            $this->engine->cleanerProcess();
        }
    }

    /**
     * @param $offset
     *
     * @return mixed
     */
    public function next($offset)
    {
        return $this->engine->getFailure()->peek($offset);
    }
}