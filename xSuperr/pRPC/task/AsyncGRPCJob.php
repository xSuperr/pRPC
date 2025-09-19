<?php

namespace xSuperr\pRPC\task;


class AsyncGRPCJob {
    public function __construct(
        private string $id,
        private string $method,
        private string $requestClass,
        private string $requestData
    ) {}

    public function getId(): string
    {
        return $this->id;
    }

    public function getRequestClass(): string
    {
        return $this->requestClass;
    }

    public function getRequestData(): string
    {
        return $this->requestData;
    }

    public function getMethod(): string
    {
        return $this->method;
    }
}