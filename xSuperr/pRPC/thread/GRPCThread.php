<?php

namespace xSuperr\pRPC\thread;

use Grpc\BaseStub;
use pocketmine\snooze\SleeperHandlerEntry;
use pocketmine\thread\Thread;
use pmmp\thread\ThreadSafeArray;
use Throwable;
use const Grpc\STATUS_OK;
use const Grpc\STATUS_UNAVAILABLE;


class GRPCThread extends Thread
{
    private ThreadSafeArray $queue;
    private ThreadSafeArray $results;

    private int $waiting = 0;

    private string $clientOptions;

    private bool $isRunning = false;

    public function __construct(private string $clientClass, private string $host, array $clientOptions, private SleeperHandlerEntry $sleeperHandlerEntry)
    {
        $this->queue = new ThreadSafeArray();
        $this->results = new ThreadSafeArray();

        $this->clientOptions = serialize($clientOptions);

        $this->start();
    }

    public function start($options = 0): bool
    {
        $this->isRunning = true;
        return parent::start($options);
    }

    public function quit(): void
    {
        $this->isRunning = false;
        parent::quit();
    }

    public function onRun(): void
    {
        $sleeperNotifier = $this->sleeperHandlerEntry->createNotifier();

        $this->registerClassLoaders();
        gc_enable();

        ini_set('display_errors', '1');
        ini_set('display_startup_errors', '1');

        $client = null;
        while ($client === null && $this->isRunning) {
            $client = $this->initClient();
            if ($client === null) {
                sleep(1);
            }
        }

        while ($this->isRunning) {
            if ($this->waiting > 0) {
                if ($client !== null) {
                    $this->processQueue($client);
                    $sleeperNotifier->wakeupSleeper();
                } else {
                    $client = $this->initClient();
                }
            }

            usleep(1000);
        }
    }

    private function initClient(): ?BaseStub
    {
        try {
            $c = new $this->clientClass($this->host, unserialize($this->clientOptions));
            if ($c instanceof BaseStub) return $c;
        } catch (\Throwable $e) {
            echo "err\n";
            var_dump($e);
            return null;
        }

        return null;
    }

    private function processQueue(?BaseStub &$client): void
    {
        $jobs = [];

        foreach ($this->queue as $k => $serialized) {
            $jobs[] = unserialize($serialized);
            unset($this->queue[$k]);
            $this->waiting--;
        }

        if (empty($jobs)) {
            return;
        }

        $calls = [];
        $results = [];

        foreach ($jobs as $job) {
            $id = $job['id'];
            var_dump($job['id']);
            try {
                $class = $job['class'];
                var_dump($class);

                $request = new $class();
                $request->mergeFromString($job['data']);
                var_dump($request);

                $method = $job['method'];
                var_dump($method);

                if ($client === null) throw new \Exception('Client is null');

                var_dump($client);

                $calls[$id] = $client->$method($request);
            } catch (Throwable $e) {
                var_dump($e);
                //$results[$id] = ['result' => "job exec: " . $e->getMessage(), 'ok' => false];
            }
        }

        foreach ($calls as $id => $call) {
            try {
                [$response, $status] = $call->wait();

                $data = ['ok' => false];
                if ($status->code === STATUS_OK) {
                    $data['result'] = $response;
                    $data['ok'] = true;

                } else {
                    if ($status->code === STATUS_UNAVAILABLE || $this->isShutdownError($status->details)) $client = $this->initClient();

                    $data['result'] = "gRPC Error [$status->code]: $status->details";
                }
                $results[$id] = $data;
            } catch (\Throwable $e) {
                $data['result'] = "Wait Exception: " . $e->getMessage();
                $results[$id] = $data;

                if ($this->isShutdownError($e->getMessage())) {
                    $client = $this->initClient();
                }
            }
        }

        var_dump($results);

        foreach ($results as $id => $data) {
            $this->results[] = serialize([
                'id' => $id,
                'result' => $data['result'],
                'ok' => $data['ok'],
            ]);
        }

       var_dump($this->results);
    }

    private function isShutdownError(string $str): bool {
        $err = strtolower($str);
        return
            str_contains($err, "shutdown") ||
            str_contains($err, "failed") ||
            str_contains($err, "unavailable") ||
            str_contains($err, "transient");
    }

    public function getWaiting(): int
    {
        return $this->waiting;
    }

    public function getResults(): ThreadSafeArray
    {
        $results = $this->results;
        $this->results = new ThreadSafeArray();
        return $results;
    }

    public function queue(string $id, string $requestClass, string $requestData, string $method): void
    {
        $this->queue[] = serialize(['id' => $id, 'class' => $requestClass, 'data' => $requestData, 'method' => $method]);
        $this->waiting++;
    }
}
