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

/**
 * Class RecurringJob
 *
 * @package AllProgrammic\Component\Resque
 */
class RecurringJob
{
    /** @var string */
    const KEY_RECURRING_JOBS = "recurring";

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
    public function setExpression(string $expression): void
    {
        $this->expression = $expression;
    }

    /**
     * @return string
     */
    public function getExpression(): string
    {
        return $this->expression;
    }


    /**
     * @return string
     */
    public function getName(): string
    {
        return $this->name;
    }

    /**
     * @param string $name
     */
    public function setName(string $name): void
    {
        $this->name = $name;
    }

    /**
     * @return string
     */
    public function getDescription(): string
    {
        return $this->description;
    }

    /**
     * @param string $description
     */
    public function setDescription(string $description): void
    {
        $this->description = $description;
    }

    /**
     * @param bool $trackStatus
     *
     * @return bool
     */
    public function schedule($trackStatus = true)
    {
        // Check if this job is valid to run.
        // If another job of the same class has been scheduled after this one was scheduled,
        if ($this->engine->hasRecurringJobs($this->queue)) {
            return false;
        }

        if (!CronExpression::isValidExpression($this->expression)) {
            throw new InvalidRecurringJobException('Cron expression is not valid');
        }

        $cron = CronExpression::factory($this->expression);
        $next = $cron->getNextRunDate();

        if ($this->engine->hasDelayedJobsAt($next)) {
            return false;
        }

        // Enqueue current job
        $this->engine->enqueueAt($next, $this->queue, $this->class, $this->args, $trackStatus);

        // Process recurring jobs
        $this->engine->processRecurringJobs($this->queue);
    }
}