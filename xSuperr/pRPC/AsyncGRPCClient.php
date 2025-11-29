<?php

namespace xSuperr\pRPC;

use Grpc\ChannelCredentials;
use pocketmine\plugin\PluginBase;
use pocketmine\scheduler\ClosureTask;
use pRPC\thread\GRPCThread;
use xSuperr\Promise\BetterPromise;
use xSuperr\Promise\BetterPromiseResolver;
use xSuperr\Promise\PromiseManager;

abstract class AsyncGRPCClient {
    private PromiseManager $manager;

    private bool $running;
    private GRPCThread $thread;

    public function __construct(PluginBase $plugin) {
        ChannelCredentials::setDefaultRootsPem($plugin->getResourceFolder() . 'roots.pem');

        $this->manager = new PromiseManager();
        $plugin->getScheduler()->scheduleRepeatingTask(new ClosureTask(fn() => $this->manager->checkTimeouts()), 20);

        $sleeperHandlerEntry = $plugin->getServer()->getTickSleeper()->addNotifier(function () {
            if (!$this->running) return;

            $results = $this->thread->getResults();
            foreach ($results as $result) {
                $result = unserialize($result);
                $ok = $result["ok"];
                if ($ok) $this->manager->resolve($result['id'], $result['result']);
                else $this->manager->reject($result['id'], $result['result']);
            }
        });

        $this->thread = new GRPCThread($this->getClientClass(), $this->getHost(), $this->getClientOptions(), $sleeperHandlerEntry);
        $this->running = true;
    }

    abstract protected function getHost(): string;
    abstract protected function getClientClass(): string;
    abstract protected function getClientOptions(): array;

    public function quit(): void
    {
        if (!$this->running) return;

        $this->waitAll();
        $this->thread->quit();
        $this->running = false;
    }

    public function isRunning(): bool
    {
        return $this->running;
    }

    /**
     * Freezes main thread until all data is flushed
     */
    public function waitAll(): void
    {
        if (!$this->running) return;

        while($this->thread->getWaiting() > 0) usleep(5000);
    }

    protected function submitGRPCJob(string $requestClass, string $requestData, string $method, int $timeout = 5): BetterPromise
    {
        if (!$this->running) {
            $resolver = new BetterPromiseResolver();
            $resolver->reject(new \Exception('gRPC client is not running'));
            return $resolver->getPromise();
        }

        [$id, $promise] = $this->manager->createPromise($timeout);

        $this->thread->queue($id, $requestClass, $requestData, $method);
        return $promise;
    }
}
