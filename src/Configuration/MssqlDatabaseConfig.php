<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\SSLConnectionConfig;
use Keboola\DbExtractorConfig\Exception\PropertyNotSetException;

class MssqlDatabaseConfig extends DatabaseConfig
{
    public const AUTH_TYPE_SQL = 'sql';
    public const AUTH_TYPE_AD_SERVICE_PRINCIPAL = 'ad_service_principal';
    public const AUTH_TYPE_AD_PASSWORD = 'ad_password';

    private const MAX_QUERY_TIMEOUT = 86400; // 24 hours

    private string $authType;

    private ?string $clientId;

    private ?string $clientSecret;

    private ?string $instance;

    private ?int $queryTimeout = null;

    public static function fromArray(array $data): self
    {
        $sslEnabled = !empty($data['ssl']) && !empty($data['ssl']['enabled']);
        $authType = empty($data['authType']) ? self::AUTH_TYPE_SQL : (string) $data['authType'];

        return new self(
            $data['host'],
            $data['instance'] ?? null,
            isset($data['port']) ? (string) $data['port'] : null,
            $authType,
            $data['user'] ?? null,
            $data['#password'] ?? null,
            $data['clientId'] ?? null,
            $data['#clientSecret'] ?? null,
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
        string $authType,
        ?string $username,
        ?string $password,
        ?string $clientId,
        ?string $clientSecret,
        ?string $database,
        ?string $schema,
        ?SSLConnectionConfig $sslConnectionConfig,
        array $initQueries,
        ?int $queryTimeout = null,
    ) {
        $this->authType = $authType;
        $this->clientId = $clientId;
        $this->clientSecret = $clientSecret;
        $this->validateCredentials($authType, $username, $password, $clientId, $clientSecret);

        // Service principal auth uses the client id / secret as the connection credentials, so the
        // legacy user/#password fields are absent. The parent value object types them as non-null
        // strings, so substitute empty placeholders (never null) — they are unused on that path.
        parent::__construct(
            $host,
            $port,
            $username ?? '',
            $password ?? '',
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

    public function getAuthType(): string
    {
        return $this->authType;
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

    private function validateCredentials(
        string $authType,
        ?string $username,
        ?string $password,
        ?string $clientId,
        ?string $clientSecret,
    ): void {
        if ($authType === self::AUTH_TYPE_AD_SERVICE_PRINCIPAL) {
            if (empty($clientId) || empty($clientSecret)) {
                throw new UserException(
                    'The "clientId" and "#clientSecret" parameters are required '
                    . 'for the "ad_service_principal" authentication type.',
                );
            }
            return;
        }

        // sql and ad_password both authenticate with the user / #password fields
        if (empty($username) || empty($password)) {
            throw new UserException(
                'The "user" and "#password" parameters are required '
                . sprintf('for the "%s" authentication type.', $authType),
            );
        }
    }
}
