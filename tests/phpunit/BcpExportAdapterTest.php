<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Adapter\Metadata\MetadataProvider;
use Keboola\DbExtractor\Configuration\MssqlDatabaseConfig;
use Keboola\DbExtractor\Extractor\Adapters\BcpExportAdapter;
use Keboola\DbExtractor\Extractor\MSSQLPdoConnection;
use Keboola\DbExtractor\Extractor\MSSQLQueryFactory;
use Keboola\DbExtractor\Extractor\ServicePrincipalTokenProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use ReflectionClass;

class BcpExportAdapterTest extends TestCase
{
    public function testSqlLoginCommand(): void
    {
        $config = MssqlDatabaseConfig::fromArray([
            'host' => 'localhost',
            'port' => '1433',
            'database' => 'test',
            'user' => 'sa',
            '#password' => 'secret',
        ]);

        $cmd = $this->buildCommand($config, null);

        $this->assertStringContainsString('-U ', $cmd);
        $this->assertStringContainsString("'sa'", $cmd);
        $this->assertStringNotContainsString('-G', $cmd);
    }

    public function testServicePrincipalCommand(): void
    {
        $config = MssqlDatabaseConfig::fromArray([
            'host' => 'server.database.windows.net',
            'database' => 'test',
            'tenantId' => 'tenant-id',
            'clientId' => 'client-id',
            '#clientSecret' => 'client-secret',
        ]);

        $tokenProvider = $this->createMock(ServicePrincipalTokenProvider::class);
        $tokenProvider->method('getAccessToken')->willReturn('fake-access-token');

        $cmd = $this->buildCommand($config, $tokenProvider);

        $this->assertStringContainsString('-G -P ', $cmd);
        $this->assertStringNotContainsString('-U ', $cmd);

        // The token file must exist and be UTF-16LE encoded (no BOM).
        $this->assertSame(1, preg_match('/-P \'([^\']+)\'/', $cmd, $m));
        $tokenFile = $m[1];
        $this->assertFileExists($tokenFile);
        $this->assertSame(
            mb_convert_encoding('fake-access-token', 'UTF-16LE', 'UTF-8'),
            (string) file_get_contents($tokenFile),
        );
        @unlink($tokenFile);
    }

    private function buildCommand(MssqlDatabaseConfig $config, ?ServicePrincipalTokenProvider $tokenProvider): string
    {
        $adapter = new BcpExportAdapter(
            new NullLogger(),
            $this->createMock(MSSQLPdoConnection::class),
            $this->createMock(MetadataProvider::class),
            $config,
            $this->createMock(MSSQLQueryFactory::class),
            $tokenProvider,
        );

        $method = (new ReflectionClass($adapter))->getMethod('createBcpCommand');
        $method->setAccessible(true);

        return (string) $method->invoke($adapter, '/tmp/output.csv', 'SELECT 1');
    }
}
