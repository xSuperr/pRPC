<?php

namespace xSuperr\pRPC;

use Grpc\ChannelCredentials;
use pocketmine\plugin\PluginBase;
use pocketmine\scheduler\AsyncPool;
use pocketmine\scheduler\ClosureTask;
use pocketmine\scheduler\TaskScheduler;
use Ramsey\Uuid\Uuid;
use SOFe\AwaitGenerator\Channel;
use xSuperr\Promise\BetterPromise;
use xSuperr\Promise\BetterPromiseResolver;
use xSuperr\pRPC\task\AsyncGRPCJob;
use xSuperr\pRPC\task\AsyncGRPCTask;

abstract class AsyncGRPCClient {
    protected array $pendingJobs = [];
    protected array $pendingResolvers = [];

    protected AsyncPool $pool;

    public function __construct(PluginBase $plugin, private int $flushInterval = 1) {
        ChannelCredentials::setDefaultRootsPem($plugin->getResourceFolder() . 'roots.pem');

        $this->pool = $plugin->getServer()->getAsyncPool();
        $this->startFlushTask($plugin->getScheduler());
    }

    private function startFlushTask(TaskScheduler $scheduler): void
    {
        $scheduler->scheduleRepeatingTask(new ClosureTask(function (): void {
            if (!empty($this->pendingJobs)) {
                $resolver = new BetterPromiseResolver();

                $task = new AsyncGRPCTask($this->getClientClass(), $this->getHost(), $this->getClientOptions(), $this->pendingJobs, $resolver);
                $this->pool->submitTask($task);
                $this->pendingJobs = [];

                $pendingResolvers = $this->pendingResolvers;
                $this->pendingResolvers = [];

                $resolver->getPromise()->then(function (?array $results) use ($pendingResolvers): void {
                    if ($results === null) {
                        foreach ($pendingResolvers as $resolver) {
                            $resolver->reject();
                        }
                    } else {
                        foreach ($results as $id => $result) {
                            if (isset($pendingResolvers[$id])) {
                                $pendingResolvers[$id]->resolve($result);
                                unset($pendingResolvers[$id]);
                            }
                        }

                        foreach ($pendingResolvers as $resolver) {
                            $resolver->reject();
                        }
                    }
                });
            }
        }), $this->flushInterval);
    }

    abstract protected function getHost(): string;
    abstract protected function getClientClass(): string;
    abstract protected function getClientOptions(): array;

    protected function submitGRPCJob(string $requestClass, string $requestData, string $method): BetterPromise
    {
        $resolver = new BetterPromiseResolver();

        $id = Uuid::uuid4()->getBytes();
        while (isset($this->pendingJobs[$id])) $id = Uuid::uuid4()->getBytes();

        $this->pendingJobs[$id] = new AsyncGRPCJob($id, $method, $requestClass, $requestData);
        $this->pendingResolvers[$id] = $resolver;
        return $resolver->getPromise();
    }
}