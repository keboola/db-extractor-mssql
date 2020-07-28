<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

class MSSQLSslTest extends AbstractMSSQLTest
{
    public function testVerifyCertValidCertificate(): void
    {
        $this->replaceConfig($this->getConfig(), true, 'ca.crt');
        $process = $this->createAppProcess();
        $process->mustRun();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals('', $process->getErrorOutput());
        $this->assertStringContainsString('Using SSL connection', $process->getOutput());
        $this->assertStringContainsString('Encrypt=true', $process->getOutput());
        $this->assertStringContainsString('TrustServerCertificate=false', $process->getOutput());
    }

    public function testVerifyCertInvalidCertificate(): void
    {
        $this->replaceConfig($this->getConfig(), true, 'invalidCa.crt');
        $process = $this->createAppProcess();
        $process->run();

        $this->assertEquals(1, $process->getExitCode());
        $this->assertStringContainsString(
            'SSL routines:tls_process_server_certificate:certificate verify failed',
            $process->getErrorOutput()
        );
    }

    public function testNotVerifyCertValidCertificate(): void
    {
        $this->replaceConfig($this->getConfig(), false, 'ca.crt');
        $process = $this->createAppProcess();
        $process->mustRun();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals('', $process->getErrorOutput());
        $this->assertStringContainsString('Using SSL connection', $process->getOutput());
        $this->assertStringContainsString('Encrypt=true', $process->getOutput());
        $this->assertStringContainsString('TrustServerCertificate=true', $process->getOutput());
    }

    public function testNotVerifyCertMissingCertificate(): void
    {
        $this->replaceConfig($this->getConfig(), false);
        $process = $this->createAppProcess();
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals('', $process->getErrorOutput());
        $this->assertStringContainsString('Using SSL connection', $process->getOutput());
        $this->assertStringContainsString('Encrypt=true', $process->getOutput());
        $this->assertStringContainsString('TrustServerCertificate=true', $process->getOutput());
    }

    public function testMissingCertificate(): void
    {
        $this->replaceConfig($this->getConfig(), true);
        $process = $this->createAppProcess();
        $process->run();

        $this->assertEquals(1, $process->getExitCode());
        $this->assertStringContainsString(
            'PEM routines:get_name:no start line:Expecting: TRUSTED CERTIFICATE',
            $process->getErrorOutput()
        );
    }

    private function replaceConfig(array $config, bool $verifyServerCert = false, ?string $ca = null): void
    {
        $config['parameters']['db']['ssl'] = [
            'enabled' => true,
            'verifyServerCert' => $verifyServerCert,
        ];
        $config['parameters']['db']['ssl']['ca'] =
            $ca ?
            file_get_contents(sprintf('%s/ssl/certs/%s', $this->dataDir, $ca)) :
            ''
        ;

        @unlink($this->dataDir . '/config.json');
        file_put_contents($this->dataDir . '/config.json', json_encode($config));
    }
}
