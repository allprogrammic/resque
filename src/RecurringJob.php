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

use AllProgrammic\Component\Resque\Cron\CronExpression;
use AllProgrammic\Component\Resque\Job\DontPerform;
use AllProgrammic\Component\Resque\Job\InvalidRecurringJobException;
use AllProgrammic\Component\Resque\Job\Status;

/**
 * Class RecurringJob
 *
 * @package AllProgrammic\Component\Resque
 */
class RecurringJob
{
    /** @var string */
    const KEY_RECURRING_JOBS = "recurring";

    /** @var string */
    const KEY_HISTORY_JOBS = "history";

    /** @var integer */
    const HISTORY_LIMIT = 30;

    /** @var Engine */
    private $engine;

    /** @var Worker */
    private $worker;

    /** @var string */
    private $queue;

    /** @var string */
    private $name;

    /** @var string */
    private $description;

    /** @var string */
    private $args;

    /** @var string */
    private $class;

    /** @var string */
    private $expression;

    /** @var integer */
    private $status = Status::STATUS_WAITING;

    /**
     * RecurringJob constructor.
     *
     * @param Engine $engine
     * @param Worker $worker
     * @param $queue
     * @param $args
     * @param $class
     */
    public function __construct(
        Engine $engine,
        Worker $worker,
        $queue,
        $args,
        $class
    ) {
        $this->engine = $engine;
        $this->worker = $worker;
        $this->queue = $queue;
        $this->args = $args;
        $this->class = $class;
    }

    /**
     * @return Engine
     */
    public function getEngine()
    {
        return $this->engine;
    }

    /**
     * @return Worker
     */
    public function getWorker()
    {
        return $this->worker;
    }

    /**
     * @return string
     */
    public function getArgs()
    {
        return $this->args;
    }

    /**
     * @return string
     */
    public function getQueue()
    {
        return $this->queue;
    }

    /**
     * @return string
     */
    public function getClass()
    {
        return $this->class;
    }

    /**
     * @param string $expression
     */
    public function setExpression(string $expression)
    {
        $this->expression = $expression;
    }

    /**
     * @return string
     */
    public function getExpression()
    {
        return $this->expression;
    }


    /**
     * @return string
     */
    public function getName()
    {
        return $this->name;
    }

    /**
     * @param string $name
     */
    public function setName(string $name)
    {
        $this->name = $name;
    }

    /**
     * @return string
     */
    public function getDescription()
    {
        return $this->description;
    }

    /**
     * @param string $description
     */
    public function setDescription(string $description)
    {
        $this->description = $description;
    }

    /**
     * @return int
     */
    public function getStatus()
    {
        return $this->status;
    }

    /**
     * @param bool $trackStatus
     *
     * @return bool
     */
    public function schedule($trackStatus = false)
    {
        if ($this->engine->hasRecurringJobs($this->name)) {
            return false;
        }

        if (!CronExpression::isValidExpression($this->expression)) {
            throw new InvalidRecurringJobException('Cron expression is not valid');
        }

        $cron = CronExpression::factory($this->expression);
        $next = $cron->getNextRunDate();

        $this->args['recurring'] = true;
        $this->args['name'] = $this->name;
        $this->args['timestamp'] = $next->getTimestamp();

        // Enqueue current job
        $this->engine->enqueueAt($next, $this->name, $this->queue, $this->class, $this->args, $trackStatus);

        // Process recurring jobs
        $this->engine->processRecurringJobs($this->name);

        // Save in history
        $this->engine->historyRecurringJobs($this, $next);
    }
}