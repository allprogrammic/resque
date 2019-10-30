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
        /** @var $attemps int */
        if (!$attemps = (int) $this->task['attempts']) {
            return false;
        }
        
        if (!isset($failure['queue'])) {
            return false;
        }

        if (!isset($failure['payload'])) {
            return false;
        }

        /** @var $payload array */
        $payload = $failure['payload'];

        if (!isset($payload['class'])) {
            return false;
        }

        if (!preg_match(sprintf('/%s/i', $this->task['class']), $payload['class'])) {
            return false;
        }

        if (!empty($this->task['exception'])) {
            if ($failure['exception'] !== $this->task['exception']) {
                return false;
            }
        }

        if (!empty($this->task['queue'])) {
            if ($this->task['queue'] !== $failure['queue']) {
                return false;
            }
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

            $payload  = $result['payload'];
            $attempts = 1;

            if (isset($payload['attempts'])) {
                $attempts = $payload['attempts'] + 1;
            }

            if ($attempts == $this->task['attempts'] && $this->task['alert']) {
                $this->engine->sendMail('failure', $result);
            }

            if ($attempts > $this->task['attempts'] || !$this->engine->removeFailureJob($offset)) {
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
