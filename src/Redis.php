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

use AllProgrammic\Component\Redis\Client;
use AllProgrammic\Component\Redis\Cluster;
use AllProgrammic\Component\Redis\Exception;

/**
 * Wrap redis client to add namespace support and various helper methods.
 *
 * Server/Connection:
 * @method Client pipeline()
 * @method Client multi()
 * @method array         exec()
 * @method string        flushAll()
 * @method string        flushDb()
 * @method array         info()
 * @method bool|array    config(string $setGet, string $key, string $value = null)
 *
 * Keys:
 * @method int           del(string $key)
 * @method int           exists(string $key)
 * @method int           expire(string $key, int $seconds)
 * @method int           expireAt(string $key, int $timestamp)
 * @method array         keys(string $key)
 * @method int           persist(string $key)
 * @method bool          rename(string $key, string $newKey)
 * @method bool          renameNx(string $key, string $newKey)
 * @method array         sort(string $key, string $arg1, string $valueN = null)
 * @method int           ttl(string $key)
 * @method string        type(string $key)
 *
 * Scalars:
 * @method int           append(string $key, string $value)
 * @method int           decr(string $key)
 * @method int           decrBy(string $key, int $decrement)
 * @method bool|string   get(string $key)
 * @method int           getBit(string $key, int $offset)
 * @method string        getRange(string $key, int $start, int $end)
 * @method string        getSet(string $key, string $value)
 * @method int           incr(string $key)
 * @method int           incrBy(string $key, int $decrement)
 * @method array         mGet(array $keys)
 * @method bool          mSet(array $keysValues)
 * @method int           mSetNx(array $keysValues)
 * @method bool          set(string $key, string $value)
 * @method int           setBit(string $key, int $offset, int $value)
 * @method bool          setEx(string $key, int $seconds, string $value)
 * @method int           setNx(string $key, string $value)
 * @method int           setRange(string $key, int $offset, int $value)
 * @method int           strLen(string $key)
 *
 * Sets:
 * @method int           sAdd(string $key, mixed $value, string $valueN = null)
 * @method int           sRem(string $key, mixed $value, string $valueN = null)
 * @method array         sMembers(string $key)
 * @method array         sUnion(mixed $keyOrArray, string $valueN = null)
 * @method array         sInter(mixed $keyOrArray, string $valueN = null)
 * @method array         sDiff(mixed $keyOrArray, string $valueN = null)
 * @method string        sPop(string $key)
 * @method int           sCard(string $key)
 * @method int           sIsMember(string $key, string $member)
 * @method int           sMove(string $source, string $dest, string $member)
 * @method string|array  sRandMember(string $key, int $count = null)
 * @method int           sUnionStore(string $dest, string $key1, string $key2 = null)
 * @method int           sInterStore(string $dest, string $key1, string $key2 = null)
 * @method int           sDiffStore(string $dest, string $key1, string $key2 = null)
 *
 * Hashes:
 * @method bool|int      hSet(string $key, string $field, string $value)
 * @method bool          hSetNx(string $key, string $field, string $value)
 * @method bool|string   hGet(string $key, string $field)
 * @method bool|int      hLen(string $key)
 * @method bool          hDel(string $key, string $field)
 * @method array         hKeys(string $key, string $field)
 * @method array         hVals(string $key)
 * @method array         hGetAll(string $key)
 * @method bool          hExists(string $key, string $field)
 * @method int           hIncrBy(string $key, string $field, int $value)
 * @method bool          hMSet(string $key, array $keysValues)
 * @method array         hMGet(string $key, array $fields)
 *
 * Lists:
 * @method array|null    blPop(string $keyN, int $timeout)
 * @method array|null    brPop(string $keyN, int $timeout)
 * @method array|null    brPoplPush(string $source, string $destination, int $timeout)
 * @method string|null   lIndex(string $key, int $index)
 * @method int           lInsert(string $key, string $beforeAfter, string $pivot, string $value)
 * @method int           lLen(string $key)
 * @method string|null   lPop(string $key)
 * @method int           lPush(string $key, mixed $value, mixed $valueN = null)
 * @method int           lPushX(string $key, mixed $value)
 * @method array         lRange(string $key, int $start, int $stop)
 * @method int           lRem(string $key, int $count, mixed $value)
 * @method bool          lSet(string $key, int $index, mixed $value)
 * @method bool          lTrim(string $key, int $start, int $stop)
 * @method string|null   rPop(string $key)
 * @method string|null   rPoplPush(string $source, string $destination)
 * @method int           rPush(string $key, mixed $value, mixed $valueN = null)
 * @method int           rPushX(string $key, mixed $value)
 *
 * Sorted Sets:
 * @method int           zCard(string $key)
 * @method array         zRangeByScore(string $key, mixed $start, mixed $stop, array $args = null)
 * @method array         zRevRangeByScore(string $key, mixed $start, mixed $stop, array $args = null)
 * @method int           zRemRangeByScore(string $key, mixed $start, mixed $stop)
 * @method array         zRange(string $key, mixed $start, mixed $stop, array $args = null)
 * @method array         zRevRange(string $key, mixed $start, mixed $stop, array $args = null)
 * TODO
 *
 * Pub/Sub
 * @method int           publish(string $channel, string $message)
 * @method int|array     pubsub(string $subCommand, $arg = NULL)
 *
 * Scripting:
 * @method string|int    script(string $command, string $arg1 = null)
 * @method string|int|array|bool eval(string $script, array $keys = NULL, array $args = NULL)
 * @method string|int|array|bool evalSha(string $script, array $keys = NULL, array $args = NULL)
 */
class Redis
{
    const DEFAULT_PORT = 6379;

    /**
     * @var array List of all commands in Redis that supply a key as their
     *  first argument. Used to prefix keys with the Resque namespace.
     */
    private $keyCommands = array(
        'exists',
        'del',
        'type',
        'keys',
        'expire',
        'ttl',
        'move',
        'set',
        'setex',
        'get',
        'getset',
        'setnx',
        'incr',
        'incrby',
        'decr',
        'decrby',
        'rpush',
        'lpush',
        'llen',
        'lrange',
        'ltrim',
        'lindex',
        'lset',
        'lrem',
        'lpop',
        'blpop',
        'rpop',
        'sadd',
        'srem',
        'spop',
        'scard',
        'sismember',
        'smembers',
        'srandmember',
        'zadd',
        'zrem',
        'zrange',
        'zrevrange',
        'zrangebyscore',
        'zcard',
        'zscore',
        'zremrangebyscore',
        'sort',
        'rename',
        'rpoplpush'
    );
    // sinterstore
    // sunion
    // sunionstore
    // sdiff
    // sdiffstore
    // sinter
    // smove
    // mget
    // msetnx
    // mset
    // renamenx

    /** @var \Closure */
    private $driverCallback;

    /** @var Client */
    private $client;

    /** @var string */
    private $namespace;

    /**
     * Redis constructor.
     *
     * @param $server
     * @param $namespace
     * @param null $database
     */
    public function __construct($server, $namespace, $database = null)
    {
        if (is_array($server)) {
            $this->driverCallback = function () use (
                $server
            ) {
                $driver = new \AllProgrammic\Component\Redis\Cluster($server);

                return $driver;
            };
        } else {
            list($host, $port, $dsnDatabase, $user, $password, $options) = self::parseDsn($server);

            // $user is not used, only $password

            // Look for known Credis_Client options
            $timeout = isset($options['timeout']) ? intval($options['timeout']) : null;
            $persistent = isset($options['persistent']) ? $options['persistent'] : '';
            $maxRetries = isset($options['max_connect_retries']) ? $options['max_connect_retries'] : 0;

            // If we have found a database in our DSN, use it instead of the `$database`
            // value passed into the constructor.
            if ($dsnDatabase !== false) {
                $database = $dsnDatabase;
            }

            $this->driverCallback = function () use (
                $host,
                $port,
                $timeout,
                $persistent,
                $database,
                $maxRetries,
                $password
            ) {
                $driver = new \AllProgrammic\Component\Redis\Client(
                    $host,
                    $port,
                    $timeout,
                    $persistent
                );

                $driver->setMaxConnectRetries($maxRetries);
                if ($password) {
                    $driver->auth($password);
                }

                if ($database !== null) {
                    $driver->select($database);
                }

                return $driver;
            };
        }

        $this->namespace = sprintf('%s:', rtrim($namespace, ':'));
    }

    public function forceClose()
    {
        $client = $this->client;
        $this->client = null;
        $client->close();
    }

    /**
     * Magic method to handle all function requests and prefix key based
     * operations with the {$this->namespace} key prefix.
     *
     * @param string $name The name of the method called.
     * @param array $args Array of supplied arguments to the method.
     * @return mixed Return value from Resident::call() based on the command.
     */
    public function __call($name, $args)
    {
        if (in_array(strtolower($name), $this->keyCommands)) {
            if (is_array($args[0])) {
                foreach ($args[0] as $i => $v) {
                    $args[0][$i] = $this->namespace . $v;
                }
            } else {
                $args[0] = $this->namespace . $args[0];
            }
        }

        try {
            return $this->getClient()->__call($name, $args);
        } catch (Exception $e) {
            return false;
        }
    }

    public function getNamespace()
    {
        return $this->namespace;
    }

    protected function getClient()
    {
        if (!$this->client) {
            $this->client = $this->driverCallback->call($this);
        }

        return $this->client;
    }

    /**
     * Remove prefix on redis key
     *
     * @param $key
     *
     * @return mixed
     */
    public function removePrefix($key)
    {
        return str_replace($this->namespace, '', $key);
    }

    /**
     * Parse a DSN string, which can have one of the following formats:
     *
     * - host:port
     * - redis://user:pass@host:port/db?option1=val1&option2=val2
     * - tcp://user:pass@host:port/db?option1=val1&option2=val2
     *
     * Note: the 'user' part of the DSN is not used.
     *
     * @param string $dsn A DSN string
     * @return array An array of DSN compotnents, with 'false' values for any unknown components. e.g.
     *               [host, port, db, user, pass, options]
     */
    protected function parseDsn($dsn)
    {
        if ($dsn == '') {
            // Use a sensible default for an empty DNS string
            $dsn = 'redis://localhost';
        }
        $parts = parse_url($dsn);

        // Check the URI scheme
        $validSchemes = array('redis', 'tcp');
        if (isset($parts['scheme']) && ! in_array($parts['scheme'], $validSchemes)) {
            throw new \InvalidArgumentException("Invalid DSN. Supported schemes are " . implode(', ', $validSchemes));
        }

        // Allow simple 'hostname' format, which `parse_url` treats as a path, not host.
        if (! isset($parts['host']) && isset($parts['path'])) {
            $parts['host'] = $parts['path'];
            unset($parts['path']);
        }

        // Extract the port number as an integer
        $port = isset($parts['port']) ? intval($parts['port']) : self::DEFAULT_PORT;

        // Get the database from the 'path' part of the URI
        $database = false;
        if (isset($parts['path'])) {
            // Strip non-digit chars from path
            $database = intval(preg_replace('/[^0-9]/', '', $parts['path']));
        }

        // Extract any 'user' and 'pass' values
        $user = isset($parts['user']) ? $parts['user'] : false;
        $pass = isset($parts['pass']) ? $parts['pass'] : false;

        // Convert the query string into an associative array
        $options = array();
        if (isset($parts['query'])) {
            // Parse the query string into an array
            parse_str($parts['query'], $options);
        }

        return array(
            $parts['host'],
            $port,
            $database,
            $user,
            $pass,
            $options,
        );
    }
}
