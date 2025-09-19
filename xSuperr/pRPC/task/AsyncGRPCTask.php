<?php

namespace xSuperr\pRPC\task;

use Phar;
use pocketmine\promise\PromiseResolver;
use pocketmine\scheduler\AsyncTask;
use pocketmine\utils\Filesystem;
use RuntimeException;
use Throwable;
use xSuperr\Promise\BetterPromiseResolver;
use const Grpc\STATUS_OK;

class AsyncGRPCTask extends AsyncTask
{
    private string $clientOptions;
    private string $jobs;

    /**
     * @param string $clientClass
     * @param string $host
     * @param array $clientOptions
     * @param AsyncGRPCJob[] $jobs
     * @param BetterPromiseResolver $resolver
     */
    public function __construct(private string $clientClass, private string $host, array $clientOptions, array $jobs, BetterPromiseResolver $resolver)
    {
        $this->clientOptions = serialize($clientOptions);
        $this->jobs = serialize($jobs);

        $this->storeLocal('resolver', $resolver);
    }

    public function onRun(): void
    {
        try {
            $client = new $this->clientClass($this->host, unserialize($this->clientOptions));
            $jobs = unserialize($this->jobs);

            $results = [];

            /**
             * @var AsyncGRPCJob $job
             */
            foreach ($jobs as $job) {
                try {
                    $class = $job->getRequestClass();

                    $request = new $class();
                    $request->mergeFromString($job->getRequestData());

                    $method = $job->getMethod();

                    [$response, $status] = $client->$method($request)->wait();

                    if ($status->code === STATUS_OK) {
                        $results[$job->getId()] = $response;
                    } else {
                        $results[$job->getId()] = "gRPC Error [$status->code]: $status->details";
                    }
                } catch (Throwable $e) {
                    $results[$job->getId()] = $e->getMessage();
                }
            }

            $this->setResult($results);
        } catch (Throwable $e) {
            $this->setResult(null);
        }
    }

    public function onCompletion(): void
    {
        /** @var PromiseResolver<true> $resolver */
        $resolver = $this->fetchLocal('resolver');
        $results = $this->getResult();
        $resolver->resolve($results);
    }
}