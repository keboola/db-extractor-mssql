<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\SSLConnectionConfig;
use Keboola\DbExtractorConfig\Exception\PropertyNotSetException;

class MssqlDatabaseConfig extends DatabaseConfig
{
    private ?string $instance;

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
}
