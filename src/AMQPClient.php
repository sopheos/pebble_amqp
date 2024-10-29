<?php

namespace Pebble\AMQP;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPChannelClosedException;
use PhpAmqpLib\Exception\AMQPConnectionBlockedException;
use PhpAmqpLib\Exception\AMQPNoDataException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;

class AMQPClient
{
    private string $host;
    private int $port;
    private string $user;
    private string $pass;
    protected string $exchange;

    private ?AMQPChannel $channel = null;

    public function __construct(
        string $host,
        int $port,
        string $user,
        string $pass,
        string $exchange,
    ) {
        $this->host = $host;
        $this->port = $port;
        $this->user = $user;
        $this->pass = $pass;
        $this->exchange = $exchange;
    }

    public function __destruct()
    {
        $this->close();
    }

    public function getConnection(): ?AMQPStreamConnection
    {
        return $this->channel?->getConnection();
    }

    public function getChannel(): ?AMQPChannel
    {
        return $this->channel;
    }

    public function start()
    {
        if (!$this->channel) {
            $connection = new AMQPStreamConnection(
                $this->host,
                $this->port,
                $this->user,
                $this->pass
            );

            $this->channel = $connection->channel();
            $this->channel->exchange_declare(exchange: $this->exchange, type: 'topic', durable: true, auto_delete: false);
        }
    }

    public function close()
    {
        $this->channel?->close();
        $this->channel?->getConnection()?->close();
    }

    public function publish(string $data, string ...$routing_keys)
    {
        $this->checkConnection();
        $msg = new AMQPMessage($data);
        $routing_key = join('.', $routing_keys);
        $this->channel->basic_publish($msg, $this->exchange, $routing_key);
    }

    public function queue(callable $callback, string $queueName, array $routing_keys = ['#'])
    {
        $this->checkConnection();

        $this->channel->queue_declare(
            queue: $queueName,
            durable: true,
            auto_delete: false
        );

        foreach ($routing_keys as $routing_key) {
            $this->channel->queue_bind(
                queue: $queueName,
                exchange: $this->exchange,
                routing_key: $routing_key
            );
        }

        $this->channel->basic_consume(queue: $queueName, callback: $callback);
    }

    public function listen(?callable $continue = null)
    {
        // Reproduction du comportement de :
        // $this->channel->consume()
        // pour définir des conditions d'arrets

        if (! $continue) {
            $continue = function () {
                return true;
            };
        }

        $this->checkConnection();
        $timeout = max($this->getConnection()->getReadTimeout(), 1);

        while ($continue() && $this->channel->is_consuming()) {
            try {
                $this->channel->wait(timeout: $timeout);
            } catch (AMQPTimeoutException $exception) {
                continue;
            } catch (AMQPNoDataException $exception) {
                continue;
            }
        }
    }

    protected function checkConnection()
    {
        $connection = $this->getConnection();
        if ($connection === null || !$connection->isConnected()) {
            throw new AMQPChannelClosedException('Channel connection is closed.');
        }
        if ($connection->isBlocked()) {
            throw new AMQPConnectionBlockedException();
        }
    }
}
