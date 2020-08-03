<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Process\Process;

class MSSQLSslInvalidCnTest extends AbstractMSSQLTest
{
    protected function tearDown(): void
    {
        parent::tearDown();
        $fs = new Filesystem();
        if ($fs->exists('/usr/local/share/ca-certificates/mssql.crt')) {
            $fs->remove('/usr/local/share/ca-certificates/mssql.crt');
        }
        Process::fromShellCommandline('update-ca-certificates --fresh')->mustRun();
    }

    public function testInvalidCnCertificate(): void
    {
        $config = $this->getConfig();
        $config['parameters']['db']['host'] = $this->getEnv(
            self::DRIVER,
            'DB_SSL_HOST_INVALID_CN',
            true
        );
        $this->replaceConfig($config, true, 'invalidCNCa.crt');
        $process = $this->createAppProcess();
        $process->run();

        $this->assertEquals(1, $process->getExitCode());
        $this->assertStringContainsString(
            'certificate verify failed:subject name does not match host name',
            $process->getErrorOutput()
        );
    }

    public function testIgnoreInvalidCnCertificate(): void
    {
        $config = $this->getConfig();
        $config['parameters']['db']['host'] = $this->getEnv(
            self::DRIVER,
            'DB_SSL_HOST_INVALID_CN',
            true
        );
        $this->replaceConfig($config, true, 'invalidCNCa.crt', true);
        $process = $this->createAppProcess();
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertStringContainsString(
            'certificate verify failed:subject name does not match host name',
            $process->getErrorOutput()
        );
    }

    public function testIgnoreInvalidCnAndInvalidCertificate(): void
    {
        $config = $this->getConfig();
        $config['parameters']['db']['host'] = $this->getEnv(
            self::DRIVER,
            'DB_SSL_HOST_INVALID_CN',
            true
        );
        $this->replaceConfig($config, true, 'invalidCa.crt', true);
        $process = $this->createAppProcess();
        $process->run();

        $this->assertEquals(1, $process->getExitCode());
        $this->assertStringContainsString(
            'unable to get local issuer certificate',
            $process->getErrorOutput()
        );
    }

    private function replaceConfig(
        array $config,
        bool $verifyServerCert = false,
        ?string $ca = null,
        bool $ignoreCertificateCn = false
    ): void {
        $config['parameters']['db']['ssl'] = [
            'enabled' => true,
            'verifyServerCert' => $verifyServerCert,
            'ignoreCertificateCn' => $ignoreCertificateCn,
        ];
        $config['parameters']['db']['ssl']['ca'] =
            $ca ?
            file_get_contents(sprintf('%s/ssl/certs/%s', $this->dataDir, $ca)) :
            ''
        ;

        @unlink($this->dataDir . '/config.json');
        file_put_contents($this->dataDir . '/config.json', json_encode($config));
    }

    public function getConfig(string $driver = self::DRIVER): array
    {
        $config = parent::getConfig($driver);
        $config['parameters']['db']['host'] = $this->getEnv(
            self::DRIVER,
            'DB_SSL_HOST_INVALID_CN',
            true
        );
        return $config;
    }
}
