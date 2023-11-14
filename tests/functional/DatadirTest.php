<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\FunctionalTests;

use Keboola\DatadirTests\DatadirTestCase;
use Keboola\DbExtractor\TraitTests\CloseSshTunnelsTrait;
use Keboola\DbExtractor\TraitTests\RemoveAllTablesTrait;
use PDO;
use RuntimeException;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Process\Process;
use Throwable;

class DatadirTest extends DatadirTestCase
{
    use RemoveAllTablesTrait;
    use CloseSshTunnelsTrait;

    protected PDO $connection;

    public string $testProjectDir;

    public string $testTempDir;

    public function getConnection(): PDO
    {
        return $this->connection;
    }

    public static function setUpBeforeClass(): void
    {
        parent::setUpBeforeClass();

        putenv('SSH_PRIVATE_KEY=' . (string) file_get_contents('/root/.ssh/id_rsa'));
        putenv('SSH_PUBLIC_KEY=' . (string) file_get_contents('/root/.ssh/id_rsa.pub'));
    }

    public function assertDirectoryContentsSame(string $expected, string $actual): void
    {
        $this->prettifyAllManifests($actual);
        $this->replaceTimestampValues($actual);
        parent::assertDirectoryContentsSame($expected, $actual);
    }

    protected function modifyConfigJsonContent(string $content): string
    {
        /** @var array<array> $config */
        $config = json_decode($content, true, 512, JSON_THROW_ON_ERROR);

        if (!empty($config['parameters']['db']['ssl']['ca'])) {
            $config['parameters']['db']['ssl']['ca'] = file_get_contents(
                sprintf(
                    '%s/ssl/certs/%s',
                    $this->temp->getTmpFolder(),
                    $config['parameters']['db']['ssl']['ca'],
                ),
            );
        }

        return parent::modifyConfigJsonContent((string) json_encode($config));
    }

    protected function setUp(): void
    {
        parent::setUp();
        putenv('KBC_COMPONENT_RUN_MODE=run');

        // Test dir, eg. "/code/tests/functional/full-load-ok"
        $this->testProjectDir = $this->getTestFileDir() . '/' . $this->dataName();
        $this->testTempDir = $this->temp->getTmpFolder();

        $configContent = file_get_contents($this->testProjectDir . '/source/data/config.json');

        /** @var array<array> $config */
        $config = json_decode((string) $configContent, true);
        preg_match('/%env\(string:([A-Z_]+)\)%/', $config['parameters']['db']['host'], $hostEnv);

        $this->connection = PdoTestConnection::createConnection(
            (string) getenv($hostEnv[1]),
        );
        $this->removeAllTables();
        $this->closeSshTunnels();

        // Load setUp.php file - used to init database state
        $setUpPhpFile = $this->testProjectDir . '/setUp.php';
        if (file_exists($setUpPhpFile)) {
            // Get callback from file and check it
            $initCallback = require $setUpPhpFile;
            if (!is_callable($initCallback)) {
                throw new RuntimeException(sprintf('File "%s" must return callback!', $setUpPhpFile));
            }

            // Invoke callback
            $initCallback($this);
        }
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $fs = new Filesystem();
        if ($fs->exists('/usr/local/share/ca-certificates/mssql.crt')) {
            $fs->remove('/usr/local/share/ca-certificates/mssql.crt');
            Process::fromShellCommandline('update-ca-certificates --fresh')->mustRun();
        }
    }

    protected function prettifyAllManifests(string $actual): void
    {
        foreach ($this->findManifests($actual . '/tables') as $file) {
            $this->prettifyJsonFile((string) $file->getRealPath());
        }
    }

    protected function prettifyJsonFile(string $path): void
    {
        $json = (string) file_get_contents($path);
        try {
            file_put_contents($path, (string) json_encode(json_decode($json), JSON_PRETTY_PRINT));
        } catch (Throwable $e) {
            // If a problem occurs, preserve the original contents
            file_put_contents($path, $json);
        }
    }

    protected function findManifests(string $dir): Finder
    {
        $finder = new Finder();
        return $finder->files()->in($dir)->name(['~.*\.manifest~']);
    }

    protected function replaceTimestampValues(string $actual): void
    {
        // In CSV
        // Eg. 0x00000000000176DD -> 0x<<RANDOM>>
        $finder = new Finder();
        $files = $finder->files()->in($actual)->name(['~.csv~']);
        foreach ($files as $file) {
            $data = (string) file_get_contents((string) $file->getRealPath());
            $data = preg_replace('~0x[0-9A-F]{16}~', '0x<<RANDOM>>', $data);
            file_put_contents((string) $file->getRealPath(), $data);
        }

        // IN state.json
        $stateJsonPath = $actual . '/state.json';
        if (file_exists($stateJsonPath)) {
            $data = (string) file_get_contents($stateJsonPath);
            $data = (string) preg_replace('~0x[0-9A-F]{16}~', '0x<<RANDOM>>', $data);
            $data = preg_replace(
                '~[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3,8}~',
                'RANDOM_TIMESTAMP',
                $data,
            );
            file_put_contents($stateJsonPath, $data);
        }
    }
}
