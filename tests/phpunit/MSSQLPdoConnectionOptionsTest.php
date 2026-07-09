<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Configuration\MssqlDatabaseConfig;
use Keboola\DbExtractor\Extractor\MSSQLPdoConnection;
use PHPUnit\Framework\TestCase;

class MSSQLPdoConnectionOptionsTest extends TestCase
{
    public function testSqlLoginOptions(): void
    {
        $config = MssqlDatabaseConfig::fromArray([
            'host' => 'localhost',
            'port' => '1433',
            'database' => 'test',
            'user' => 'sa',
            '#password' => 'secret',
        ]);

        $options = MSSQLPdoConnection::buildConnectionOptions($config);

        $this->assertSame('localhost,1433', $options['Server']);
        $this->assertSame('test', $options['Database']);
        $this->assertSame('true', $options['TrustServerCertificate']);
        $this->assertArrayNotHasKey('Authentication', $options);
    }

    public function testServicePrincipalOptions(): void
    {
        $config = MssqlDatabaseConfig::fromArray([
            'host' => 'server.database.windows.net',
            'database' => 'test',
            'tenantId' => 'tenant-id',
            'clientId' => 'client-id',
            '#clientSecret' => 'client-secret',
        ]);

        $options = MSSQLPdoConnection::buildConnectionOptions($config);

        $this->assertSame('ActiveDirectoryServicePrincipal', $options['Authentication']);
        $this->assertSame('true', $options['Encrypt']);
    }
}
