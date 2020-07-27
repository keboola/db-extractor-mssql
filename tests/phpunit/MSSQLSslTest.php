<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

class MSSQLSslTest extends AbstractMSSQLTest
{
    /**
     * @dataProvider configProvider
     */
    public function testRunAction(array $config): void
    {
        $this->replaceConfig($config);
        $process = $this->createAppProcess();
        $process->mustRun();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals('', $process->getErrorOutput());
        $this->assertStringContainsString('Using SSL connection', $process->getOutput());
        $this->assertStringContainsString('Encrypt=true', $process->getOutput());
        $this->assertStringContainsString('TrustServerCertificate=false', $process->getOutput());
    }

    private function replaceConfig(array $config): void
    {
        $config['parameters']['db']['ssl'] = [
            'enabled' => true,
            'verifyServerCert' => true,
            'cert' => file_get_contents($this->dataDir . '/ssl/certs/mssql.pem'),
        ];

        @unlink($this->dataDir . '/config.json');
        file_put_contents($this->dataDir . '/config.json', json_encode($config));
    }
}
