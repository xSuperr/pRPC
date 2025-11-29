<?php

namespace xSuperr\pRPC;

use Exception;
use Google\Protobuf\Internal\Message;
use Grpc\ChannelCredentials;
use pocketmine\plugin\PluginBase;
use pocketmine\scheduler\ClosureTask;
use xSuperr\pRPC\thread\GRPCThread;
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
                $resp = $result['result'];
                $c = $resp['c'] ?? null;

                $message = null;
                $cont = true;
                if ($c !== null) {
                    try {
                        $msg = new $c();
                        if ($msg instanceof Message) {
                            $msg->mergeFromString($resp['m']);
                            $message = $msg;
                        }
                    } catch (\Throwable $e) {
                        $cont = false;
                        $this->manager->reject($result['id'], $e);
                    }
                } else $message = $resp['m'];

                if ($message === null) $this->manager->reject($result['id'], new Exception('Message is null'));
                else if ($cont) {
                    $ok = $result["ok"];
                    if ($ok) $this->manager->resolve($result['id'], $message);
                    else $this->manager->reject($result['id'], new Exception($message));
                }
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
