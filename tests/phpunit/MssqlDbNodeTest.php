<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Configuration\NodeDefinition\MssqlDbNode;
use Keboola\DbExtractor\Configuration\NodeDefinition\MssqlSslNode;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Config\Definition\Processor;

class MssqlDbNodeTest extends TestCase
{
    /**
     * @param array<string, mixed> $config
     * @dataProvider validConfigProvider
     */
    public function testValidConfig(array $config): void
    {
        $result = $this->process($config);
        $this->assertSame($config['host'], $result['host']);
    }

    public function validConfigProvider(): array
    {
        return [
            'sql-login' => [[
                'host' => 'localhost',
                'database' => 'test',
                'user' => 'sa',
                '#password' => 'secret',
            ]],
            'service-principal' => [[
                'host' => 'server.database.windows.net',
                'database' => 'test',
                'tenantId' => 'tenant-id',
                'clientId' => 'client-id',
                '#clientSecret' => 'client-secret',
            ]],
        ];
    }

    public function testMissingAuthIsInvalid(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->process([
            'host' => 'localhost',
            'database' => 'test',
        ]);
    }

    public function testPartialServicePrincipalIsInvalid(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->process([
            'host' => 'server.database.windows.net',
            'database' => 'test',
            'clientId' => 'client-id',
        ]);
    }

    /**
     * @param array<string, mixed> $config
     * @return array<string, mixed>
     */
    private function process(array $config): array
    {
        $node = (new MssqlDbNode(null, new MssqlSslNode()))->getNode();
        return (new Processor())->process($node, [$config]);
    }
}
