<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Configuration\MssqlDatabaseConfig;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Extractor\MSSQLPdoConnection;
use PHPUnit\Framework\TestCase;

/**
 * DSN / credential construction unit tests. A live Entra ID connection cannot be exercised in CI
 * (no Azure tenant / credentials), so these assert the connection string and PDO arguments that the
 * component builds for each auth type — including that SQL auth is byte-identical to the previous
 * behaviour.
 */
class MSSQLPdoConnectionTest extends TestCase
{
    public function testSqlAuthOptionsAndDsnUnchanged(): void
    {
        $config = MssqlDatabaseConfig::fromArray([
            'host' => 'mssql',
            'port' => 1433,
            'user' => 'sa',
            '#password' => 'Password.1',
            'database' => 'test',
        ]);

        $options = MSSQLPdoConnection::buildConnectionOptions($config);

        self::assertSame(['Server' => 'mssql,1433', 'Database' => 'test'], $options);
        self::assertArrayNotHasKey('Authentication', $options);
        self::assertSame('sqlsrv:Server=mssql,1433;Database=test', MSSQLPdoConnection::buildDsn($options));
        self::assertSame(['sa', 'Password.1'], MSSQLPdoConnection::resolveCredentials($config));
    }

    public function testSqlAuthWithSslUnchanged(): void
    {
        $config = MssqlDatabaseConfig::fromArray([
            'host' => 'mssql',
            'user' => 'sa',
            '#password' => 'Password.1',
            'database' => 'test',
            'ssl' => ['enabled' => true, 'verifyServerCert' => false],
        ]);

        $options = MSSQLPdoConnection::buildConnectionOptions($config);

        self::assertSame(
            ['Server' => 'mssql', 'Database' => 'test', 'Encrypt' => 'true', 'TrustServerCertificate' => 'true'],
            $options,
        );
        self::assertArrayNotHasKey('Authentication', $options);
    }

    public function testServicePrincipalOptionsAndCredentials(): void
    {
        $config = MssqlDatabaseConfig::fromArray([
            'host' => 'example.datawarehouse.fabric.microsoft.com',
            'authType' => MssqlDatabaseConfig::AUTH_TYPE_AD_SERVICE_PRINCIPAL,
            'clientId' => 'app-client-id',
            '#clientSecret' => 'the-secret',
            'database' => 'MyWarehouse',
        ]);

        $options = MSSQLPdoConnection::buildConnectionOptions($config);

        self::assertSame('ActiveDirectoryServicePrincipal', $options['Authentication']);
        self::assertSame('true', $options['Encrypt']);
        self::assertArrayNotHasKey('TrustServerCertificate', $options);
        self::assertStringContainsString(
            'Authentication=ActiveDirectoryServicePrincipal',
            MSSQLPdoConnection::buildDsn($options),
        );
        // client id / secret become the PDO UID / PWD; they are never placed in the DSN string.
        self::assertSame(['app-client-id', 'the-secret'], MSSQLPdoConnection::resolveCredentials($config));
        self::assertStringNotContainsString('the-secret', MSSQLPdoConnection::buildDsn($options));
    }

    public function testActiveDirectoryPasswordOptionsAndCredentials(): void
    {
        $config = MssqlDatabaseConfig::fromArray([
            'host' => 'server.database.windows.net',
            'authType' => MssqlDatabaseConfig::AUTH_TYPE_AD_PASSWORD,
            'user' => 'user@contoso.com',
            '#password' => 'Password.1',
            'database' => 'test',
        ]);

        $options = MSSQLPdoConnection::buildConnectionOptions($config);

        self::assertSame('ActiveDirectoryPassword', $options['Authentication']);
        self::assertSame('true', $options['Encrypt']);
        self::assertSame(['user@contoso.com', 'Password.1'], MSSQLPdoConnection::resolveCredentials($config));
    }

    public function testServicePrincipalRequiresClientCredentials(): void
    {
        $this->expectException(UserException::class);
        $this->expectExceptionMessage('"clientId" and "#clientSecret"');

        MssqlDatabaseConfig::fromArray([
            'host' => 'server',
            'authType' => MssqlDatabaseConfig::AUTH_TYPE_AD_SERVICE_PRINCIPAL,
            'database' => 'test',
        ]);
    }

    public function testSqlAuthRequiresUserAndPassword(): void
    {
        $this->expectException(UserException::class);
        $this->expectExceptionMessage('"user" and "#password"');

        MssqlDatabaseConfig::fromArray([
            'host' => 'server',
            'database' => 'test',
        ]);
    }

    public function testDefaultAuthTypeIsSql(): void
    {
        $config = MssqlDatabaseConfig::fromArray([
            'host' => 'mssql',
            'user' => 'sa',
            '#password' => 'Password.1',
            'database' => 'test',
        ]);

        self::assertSame(MssqlDatabaseConfig::AUTH_TYPE_SQL, $config->getAuthType());
    }
}
