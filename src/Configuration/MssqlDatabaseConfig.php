<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\SSLConnectionConfig;
use Keboola\DbExtractorConfig\Exception\PropertyNotSetException;

class MssqlDatabaseConfig extends DatabaseConfig
{
    private const MAX_QUERY_TIMEOUT = 86400; // 24 hours

    private ?string $instance;

    private ?int $queryTimeout = null;

    public static function fromArray(array $data): self
    {
        $sslEnabled = !empty($data['ssl']) && !empty($data['ssl']['enabled']);

        return new self(
            $data['host'],
            $data['instance'] ?? null,
            isset($data['port']) ? (string) $data['port'] : null,
            $data['user'],
            $data['#password'],
            $data['database'] ?? null,
            $data['schema'] ?? null,
            $sslEnabled ? SSLConnectionConfig::fromArray($data['ssl']) : null,
            $data['initQueries'] ?? [],
            $data['queryTimeout'] ?? null,
        );
    }

    public function __construct(
        string $host,
        ?string $instance,
        ?string $port,
        string $username,
        string $password,
        ?string $database,
        ?string $schema,
        ?SSLConnectionConfig $sslConnectionConfig,
        array $initQueries,
        ?int $queryTimeout = null,
    ) {
        parent::__construct(
            $host,
            $port,
            $username,
            $password,
            $database,
            $schema,
            $sslConnectionConfig,
            $initQueries,
        );

        $this->instance = $instance;
        if ($queryTimeout !== null) {
            $normalizedQueryTimeout = min(abs($queryTimeout), self::MAX_QUERY_TIMEOUT);
            $this->queryTimeout = $normalizedQueryTimeout ?: null;
        }
    }

    public function hasInstance(): bool
    {
        return $this->instance !== null;
    }

    public function getInstance(): string
    {
        if ($this->instance === null) {
            throw new PropertyNotSetException('Instance is not set.');
        }
        return $this->instance;
    }

    public function getQueryTimeout(): ?int
    {
        return $this->queryTimeout;
    }
}
