<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\FunctionalTests;

use Keboola\DatadirTests\DatadirTestCase;
use Keboola\DbExtractor\TraitTests\CloseSshTunnelsTrait;
use Keboola\DbExtractor\TraitTests\RemoveAllTablesTrait;
use PDO;
use RuntimeException;

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

    protected function modifyConfigJsonContent(string $content): string
    {
        $config = json_decode($content, true, 512, JSON_THROW_ON_ERROR);

        if (isset($config['parameters']['db']['ssh']['keys'])) {
            $config['parameters']['db']['ssh']['keys'] = [
                '#private' => (string) file_get_contents('/root/.ssh/id_rsa'),
                'public' => (string) file_get_contents('/root/.ssh/id_rsa.pub'),
            ];
        }

        return parent::modifyConfigJsonContent((string) json_encode($config));
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Test dir, eg. "/code/tests/functional/full-load-ok"
        $this->testProjectDir = $this->getTestFileDir() . '/' . $this->dataName();
        $this->testTempDir = $this->temp->getTmpFolder();

        $this->connection = PdoTestConnection::createConnection();
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
}
