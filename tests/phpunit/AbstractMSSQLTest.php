<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use PDO;
use SplFileInfo;
use Monolog\Logger;
use Symfony\Component\Process\Process;
use Keboola\Csv\CsvReader;
use Keboola\DbExtractor\Metadata\MssqlMetadataProvider;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\TableBuilder;
use Keboola\DbExtractor\Extractor\MetadataProvider;
use Keboola\DbExtractor\Extractor\PdoConnection;
use Keboola\DbExtractor\Extractor\QueryFactory;
use Keboola\DbExtractor\MSSQLApplication;
use Keboola\DbExtractor\Test\ExtractorTest;

abstract class AbstractMSSQLTest extends ExtractorTest
{
    public const DRIVER = 'mssql';

    /** @var PDO */
    protected $pdo;

    protected string $dataDir = __DIR__ . '/data';

    protected function setUp(): void
    {
        if (!($this->pdo instanceof PDO)) {
            $this->makeConnection();
        }
        $this->setupTables();
        $this->cleanupStateInDataDir();
    }

    protected function tearDown(): void
    {
        parent::tearDown();

        # Close SSH tunnel if created
        $process = new Process(['sh', '-c', 'pgrep ssh | xargs -r kill']);
        $process->mustRun();
    }

    private function makeConnection(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $params = $config['parameters']['db'];

        if (isset($params['#password'])) {
            $params['password'] = $params['#password'];
        }

        // create test database
        $this->pdo = new \PDO(
            sprintf('sqlsrv:Server=%s', $params['host']),
            $params['user'],
            $params['password']
        );
        $this->pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        $this->pdo->exec('USE master');
        $this->pdo->exec(sprintf("
            IF NOT EXISTS(select * from sys.databases where name='%s') 
            CREATE DATABASE %s
        ", $params['database'], $params['database']));
        $this->pdo->exec(sprintf('USE %s', $params['database']));
    }

    private function cleanupStateInDataDir(): void
    {
        @unlink($this->dataDir . '/in/state.json');
    }

    private function setupTables(): void
    {
        $salesCsv = new SplFileInfo($this->dataDir . '/mssql/sales.csv');
        $specialCsv = new SplFileInfo($this->dataDir . '/mssql/special.csv');

        $this->dropTable('Empty Test');
        $this->dropTable('simple');
        $this->dropTable('sales2');
        $this->dropTable('sales');
        $this->dropTable('special');

        $this->createTextTable($salesCsv, ['createdat'], 'sales');
        $this->createTextTable($salesCsv, null, 'sales2');
        $this->createTextTable($specialCsv, null, 'special');
        // drop the t1 demo table if it exists
        $this->dropTable('t1');

        // set up a foreign key relationship
        $this->pdo->exec('ALTER TABLE sales2 ALTER COLUMN createdat varchar(64) NOT NULL');
        $this->pdo->exec(
            'ALTER TABLE sales2 ADD CONSTRAINT FK_sales_sales2 FOREIGN KEY (createdat) REFERENCES sales(createdat)'
        );

        // create another table with an auto_increment ID
        $this->dropTable('auto Increment Timestamp');

        $this->pdo->exec(
            "CREATE TABLE [auto Increment Timestamp] (
            \"_Weir%d I-D\" INT IDENTITY(1,1) NOT NULL, 
            \"Weir%d Na-me\" VARCHAR(55) NOT NULL DEFAULT 'mario',
            \"someInteger\" INT,
            \"someDecimal\" DECIMAL(10,2),
            \"type\" VARCHAR(55) NULL,
            \"smalldatetime\" SMALLDATETIME DEFAULT NULL,
            \"datetime\" DATETIME NOT NULL DEFAULT GETDATE(),
            \"timestamp\" TIMESTAMP
            )"
        );

        // create table simple
        $this->pdo->exec('CREATE TABLE [simple] ("id" INT, "name" varchar(100), PRIMARY KEY ("id"))');

        // phpcs:disable Generic.Files.LineLength
        $this->pdo->exec('ALTER TABLE [auto Increment Timestamp] ADD CONSTRAINT PK_AUTOINC PRIMARY KEY ("_Weir%d I-D")');
        $this->pdo->exec('ALTER TABLE [auto Increment Timestamp] ADD CONSTRAINT CHK_ID_CONTSTRAINT CHECK ("_Weir%d I-D" > 0 AND "_Weir%d I-D" < 20)');
        $this->pdo->exec("INSERT INTO [auto Increment Timestamp] (\"Weir%d Na-me\", Type, someInteger, someDecimal, smalldatetime) VALUES ('mario', 'plumber', 1, 1.1, '2012-01-10 10:00')");
        $this->pdo->exec("INSERT INTO [auto Increment Timestamp] (\"Weir%d Na-me\", Type, someInteger, someDecimal, smalldatetime) VALUES ('luigi', 'plumber', 2, 2.2, '2012-01-10 10:05')");
        $this->pdo->exec("INSERT INTO [auto Increment Timestamp] (\"Weir%d Na-me\", Type, someInteger, someDecimal, smalldatetime) VALUES ('toad', 'mushroom', 3, 3.3, '2012-01-10 10:10')");
        $this->pdo->exec("INSERT INTO [auto Increment Timestamp] (\"Weir%d Na-me\", Type, someInteger, someDecimal, smalldatetime) VALUES ('princess', 'royalty', 4, 4.4, '2012-01-10 10:15')");
        $this->pdo->exec("INSERT INTO [auto Increment Timestamp] (\"Weir%d Na-me\", Type, someInteger, someDecimal, smalldatetime) VALUES ('wario', 'badguy', 5, 5.5, '2012-01-10 10:25')");
        sleep(1); // stagger the timestamps
        $this->pdo->exec("INSERT INTO [auto Increment Timestamp] (\"Weir%d Na-me\", Type, someInteger, someDecimal, smalldatetime) VALUES ('yoshi', 'horse?', 6, 6.6, '2012-01-10 10:25')");
        // add unique key
        $this->pdo->exec('ALTER TABLE [auto Increment Timestamp] ADD CONSTRAINT UNI_KEY_1 UNIQUE ("Weir%d Na-me", Type)');
        // phpcs:enable Generic.Files.LineLength
    }

    protected function dropTable(string $tableName, ?string $schema = 'dbo'): void
    {
        $this->pdo->exec(
            sprintf(
                "IF OBJECT_ID('[%s].[%s]', 'U') IS NOT NULL DROP TABLE [%s].[%s]",
                $schema,
                $tableName,
                $schema,
                $tableName
            )
        );
    }

    public function getConfig(string $driver = self::DRIVER): array
    {
        $config = parent::getConfig($driver);
        $config['parameters']['extractor_class'] = 'MSSQL';
        return $config;
    }

    protected function generateTableName(SplFileInfo $file): string
    {
        $tableName = sprintf(
            '%s',
            $file->getBasename('.' . $file->getExtension())
        );

        return 'dbo.' . $tableName;
    }

    protected function createTextTable(SplFileInfo $file, ?array $primaryKey = null, ?string $tableName = null): void
    {
        $tableName = $tableName ?: $this->generateTableName($file);
        $reader = new CsvReader($file->getPathname());

        $sql = sprintf(
            'CREATE TABLE %s (%s)',
            $tableName,
            implode(
                ', ',
                array_map(
                    function ($column) {
                        return $column . ' text NULL';
                    },
                    $reader->getHeader()
                )
            )
        );
        $this->pdo->exec($sql);
        // create the primary key if supplied
        if ($primaryKey && is_array($primaryKey) && !empty($primaryKey)) {
            foreach ($primaryKey as $pk) {
                $sql = sprintf('ALTER TABLE %s ALTER COLUMN %s varchar(64) NOT NULL', $tableName, $pk);
                $this->pdo->exec($sql);
            }

            $sql = sprintf(
                'ALTER TABLE %s ADD CONSTRAINT PK_%s PRIMARY KEY (%s)',
                $tableName,
                $tableName,
                implode(',', $primaryKey)
            );
            $this->pdo->exec($sql);
        }

        $reader->next();

        $this->pdo->beginTransaction();

        $columnsCount = count($reader->current());
        $rowsPerInsert = intval((1000 / $columnsCount) - 1);

        while ($reader->current() !== false) {
            $sqlInserts = '';

            for ($i=0; $i<$rowsPerInsert && $reader->current() !== false; $i++) {
                $sqlInserts = '';

                $sqlInserts .= sprintf(
                    '(%s),',
                    implode(
                        ',',
                        array_map(
                            function ($data) {
                                if ($data === '') {
                                    return 'null';
                                }
                                if (is_numeric($data)) {
                                    return "'" . $data . "'";
                                }

                                $nonDisplayables = array(
                                '/%0[0-8bcef]/',            // url encoded 00-08, 11, 12, 14, 15
                                '/%1[0-9a-f]/',             // url encoded 16-31
                                '/[\x00-\x08]/',            // 00-08
                                '/\x0b/',                   // 11
                                '/\x0c/',                   // 12
                                '/[\x0e-\x1f]/'             // 14-31
                                );
                                foreach ($nonDisplayables as $regex) {
                                    $data = (string) preg_replace($regex, '', $data);
                                }

                                $data = (string) str_replace("'", "''", $data);

                                return "'" . $data . "'";
                            },
                            $reader->current()
                        )
                    )
                );
                $reader->next();

                $sql = sprintf(
                    'INSERT INTO %s VALUES %s',
                    $tableName,
                    substr($sqlInserts, 0, -1)
                );

                $this->pdo->exec($sql);
            }
        }

        $this->pdo->commit();

        $count = $this->pdo->query(sprintf('SELECT COUNT(*) AS itemsCount FROM %s', $tableName))->fetchColumn();
        $this->assertEquals($this->countTable($reader), (int) $count);
    }

    protected function countTable(CsvReader $file): int
    {
        $linesCount = 0;
        foreach ($file as $i => $line) {
            // skip header
            if (!$i) {
                continue;
            }
            $linesCount++;
        }
        return $linesCount;
    }

    public function createApplication(array $config, array $state = []): MSSQLApplication
    {
        $logger = new Logger('ex-db-mssql-tests');
        $app = new MSSQLApplication($config, $logger, $state, $this->dataDir);
        return $app;
    }

    public function tableExists(string $tableName): bool
    {
        $res = $this->pdo->query(
            sprintf(
                'SELECT * FROM information_schema.tables WHERE TABLE_NAME = %s',
                $this->pdo->quote($tableName)
            )
        );
        return !($res->rowCount() === 0);
    }

    public function configProvider(): array
    {
        return [
            [
                $this->getConfig(self::DRIVER),
            ],
            [
                $this->getConfigRow(self::DRIVER),
            ],
        ];
    }

    public function getPrivateKey(): string
    {
        return (string) file_get_contents('/root/.ssh/id_rsa');
    }

    public function getPublicKey(): string
    {
        return (string) file_get_contents('/root/.ssh/id_rsa.pub');
    }

    protected function createQueryFactory(array $params, array $state, ?array $columnsMetadata = null): QueryFactory
    {
        $logger = new Logger('mssql-extractor-test');
        $pdo = new PdoConnection($logger, DatabaseConfig::fromArray($params['db']));
        if ($columnsMetadata === null) {
            $metadataProvider = new MssqlMetadataProvider($pdo);
        } else {
            $tableBuilder = TableBuilder::create()
                ->setName('mocked')
                ->setType('mocked');

            foreach ($columnsMetadata as $data) {
                $tableBuilder
                    ->addColumn()
                    ->setName($data['name'])
                    ->setType($data['type'])
                    ->setLength($data['length'] ?? null);
            }

            $tableMetadata = $tableBuilder->build();
            $metadataProviderMock = $this->createMock(MssqlMetadataProvider::class);
            $metadataProviderMock
                ->method('getTable')
                ->willReturn($tableMetadata);
            /** @var MetadataProvider $metadataProvider */
            $metadataProvider = $metadataProviderMock;
        }

        return new QueryFactory($pdo, $metadataProvider, $state);
    }

    protected function createAppProcess(): Process
    {
        $process = Process::fromShellCommandline('php /code/src/run.php', null, [
            'KBC_DATADIR' => $this->dataDir,
        ]);
        $process->setTimeout(300);
        return $process;
    }
}
