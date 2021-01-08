<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\FunctionalTests;

use Keboola\DatadirTests\DatadirTestCase;
use Keboola\DbExtractor\TraitTests\CloseSshTunnelsTrait;
use Keboola\DbExtractor\TraitTests\RemoveAllTablesTrait;
use PDO;
use RuntimeException;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Process\Process;

class DatadirTest extends DatadirTestCase
{
    use RemoveAllTablesTrait;
    use CloseSshTunnelsTrait;

    protected PDO $connection;

    protected string $testProjectDir;

    protected string $testTempDir;

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

    protected function modifyConfigJsonContent(string $content): string
    {
        $config = json_decode($content, true, 512, JSON_THROW_ON_ERROR);

        if (!empty($config['parameters']['db']['ssl']['ca'])) {
            $config['parameters']['db']['ssl']['ca'] = file_get_contents(
                sprintf(
                    '%s/ssl/certs/%s',
                    $this->temp->getTmpFolder(),
                    $config['parameters']['db']['ssl']['ca']
                )
            );
        }

        return parent::modifyConfigJsonContent((string) json_encode($config));
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Test dir, eg. "/code/tests/functional/full-load-ok"
        $this->testProjectDir = $this->getTestFileDir() . '/' . $this->dataName();
        $this->testTempDir = $this->temp->getTmpFolder();

        $configContent = file_get_contents($this->testProjectDir . '/source/data/config.json');

        $config = json_decode((string) $configContent, true);
        preg_match('/%env\(string:([A-Z_]+)\)%/', $config['parameters']['db']['host'], $hostEnv);

        $this->connection = PdoTestConnection::createConnection(
            (string) getenv($hostEnv[1])
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
}
