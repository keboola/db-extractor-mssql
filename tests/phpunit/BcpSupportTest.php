<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Configuration\MssqlDatabaseConfig;
use Keboola\DbExtractor\Extractor\MSSQL;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use PHPUnit\Framework\TestCase;

/**
 * The bcp fast-export path must stay enabled for SQL authentication (unchanged behaviour) and be
 * disabled for Microsoft Entra ID auth types, because the bcp CLI cannot authenticate with Entra.
 */
class BcpSupportTest extends TestCase
{
    public function testBcpEnabledForSqlAuth(): void
    {
        $config = MssqlDatabaseConfig::fromArray([
            'host' => 'mssql',
            'user' => 'sa',
            '#password' => 'Password.1',
            'database' => 'test',
        ]);

        self::assertTrue(MSSQL::isBcpSupported($config));
    }

    public function testBcpDisabledForServicePrincipalAuth(): void
    {
        $config = MssqlDatabaseConfig::fromArray([
            'host' => 'example.datawarehouse.fabric.microsoft.com',
            'authType' => MssqlDatabaseConfig::AUTH_TYPE_AD_SERVICE_PRINCIPAL,
            'clientId' => 'app-client-id',
            '#clientSecret' => 'the-secret',
            'tenantId' => 'the-tenant',
            'database' => 'MyWarehouse',
        ]);

        self::assertFalse(MSSQL::isBcpSupported($config));
    }

    public function testBcpDisabledForAdPasswordAuth(): void
    {
        $config = MssqlDatabaseConfig::fromArray([
            'host' => 'server.database.windows.net',
            'authType' => MssqlDatabaseConfig::AUTH_TYPE_AD_PASSWORD,
            'user' => 'user@contoso.com',
            '#password' => 'Password.1',
            'database' => 'test',
        ]);

        self::assertFalse(MSSQL::isBcpSupported($config));
    }

    public function testBcpEnabledForNonMssqlConfig(): void
    {
        $config = DatabaseConfig::fromArray([
            'host' => 'mssql',
            'user' => 'sa',
            '#password' => 'Password.1',
            'database' => 'test',
        ]);

        self::assertTrue(MSSQL::isBcpSupported($config));
    }
}
