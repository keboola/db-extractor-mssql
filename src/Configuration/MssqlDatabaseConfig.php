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

    private ?string $tenantId;

    private ?string $clientId;

    private ?string $clientSecret;

    public static function fromArray(array $data): self
    {
        $sslEnabled = !empty($data['ssl']) && !empty($data['ssl']['enabled']);

        return new self(
            $data['host'],
            $data['instance'] ?? null,
            isset($data['port']) ? (string) $data['port'] : null,
            $data['user'] ?? '',
            $data['#password'] ?? '',
            $data['database'] ?? null,
            $data['schema'] ?? null,
            $sslEnabled ? SSLConnectionConfig::fromArray($data['ssl']) : null,
            $data['initQueries'] ?? [],
            $data['queryTimeout'] ?? null,
            $data['tenantId'] ?? null,
            $data['clientId'] ?? null,
            $data['#clientSecret'] ?? null,
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
        ?string $tenantId = null,
        ?string $clientId = null,
        ?string $clientSecret = null,
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
        $this->tenantId = $tenantId ?: null;
        $this->clientId = $clientId ?: null;
        $this->clientSecret = $clientSecret ?: null;
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

    /**
     * Azure AD Service Principal auth is used when all three credentials are present.
     */
    public function hasServicePrincipal(): bool
    {
        return $this->tenantId !== null && $this->clientId !== null && $this->clientSecret !== null;
    }

    public function getTenantId(): string
    {
        if ($this->tenantId === null) {
            throw new PropertyNotSetException('Property "tenantId" is not set.');
        }
        return $this->tenantId;
    }

    public function getClientId(): string
    {
        if ($this->clientId === null) {
            throw new PropertyNotSetException('Property "clientId" is not set.');
        }
        return $this->clientId;
    }

    public function getClientSecret(): string
    {
        if ($this->clientSecret === null) {
            throw new PropertyNotSetException('Property "#clientSecret" is not set.');
        }
        return $this->clientSecret;
    }

    /**
     * Username passed to the PDO/ODBC driver. For Service Principal auth this is the client ID.
     */
    public function getConnectionUsername(): string
    {
        return $this->hasServicePrincipal() ? $this->getClientId() : $this->getUsername();
    }

    /**
     * Password passed to the PDO/ODBC driver. For Service Principal auth this is the client secret.
     */
    public function getConnectionPassword(): string
    {
        return $this->hasServicePrincipal() ? $this->getClientSecret() : $this->getPassword();
    }
}
