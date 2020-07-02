<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor\Adapters;

use Throwable;
use PDO;
use PDOException;
use Psr\Log\LoggerInterface;
use Retry\RetryProxy;
use Keboola\Csv\CsvWriter;
use Keboola\DbExtractor\Extractor\PdoConnection;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbExtractor\DbRetryProxy;
use Keboola\DbExtractor\Exception\UserException;

class PdoAdapter
{
    private LoggerInterface $logger;

    private PdoConnection $pdo;

    private array $state;

    public function __construct(LoggerInterface $logger, PdoConnection $pdo, array $state)
    {
        $this->logger = $logger;
        $this->pdo = $pdo;
        $this->state = $state;
    }

    public function export(array $table, string $query, ?array $incrementalFetching, string $csvPath): array
    {
        $isAdvancedQuery = array_key_exists('query', $table);
        $maxTries = isset($table['retries']) ? (int) $table['retries'] : Extractor::DEFAULT_MAX_TRIES;

        // Check connection
        $this->pdo->tryReconnect();

        return $this
            ->createRetryProxy($maxTries)
            ->call(function () use ($query, $isAdvancedQuery, $incrementalFetching, $csvPath) {
                try {
                    // Csv writer must be re-created after each error, because some lines could be already written
                    $csv = new CsvFile($csvPath);
                    $result =  $this->executeAndWrite($query, $isAdvancedQuery, $incrementalFetching, $csv);
                    $this->pdo->isAlive();
                    return $result;
                } catch (Throwable $queryError) {
                    try {
                        $this->pdo->connect();
                    } catch (Throwable $connectionError) {
                    };
                    throw $queryError;
                }
            });
    }

    private function executeAndWrite(
        string $query,
        bool $includeHeader,
        ?array $incrementalFetching,
        CsvFile $csv
    ): array {
        $stmt = $this->pdo->prepare($query);
        $stmt->execute();

        $output = [];

        $resultRow = $stmt->fetch(PDO::FETCH_ASSOC);

        if (is_array($resultRow) && !empty($resultRow)) {
            // write header and first line
            if ($includeHeader) {
                $csv->writeRow(array_keys($resultRow));
            }
            $csv->writeRow($resultRow);

            // write the rest
            $numRows = 1;
            $lastRow = $resultRow;

            while ($resultRow = $stmt->fetch(PDO::FETCH_ASSOC)) {
                $csv->writeRow($resultRow);
                $lastRow = $resultRow;
                $numRows++;
            }
            $stmt->closeCursor();

            if (isset($incrementalFetching['column'])) {
                if (!array_key_exists($incrementalFetching['column'], $lastRow)) {
                    throw new UserException(
                        sprintf(
                            'The specified incremental fetching column %s not found in the table',
                            $incrementalFetching['column']
                        )
                    );
                }
                $output['lastFetchedRow'] = $lastRow[$incrementalFetching['column']];
            }
            $output['rows'] = $numRows;
            return $output;
        }

        $output['rows'] = 0;
        return $output;
    }

    private function createRetryProxy(int $maxTries): RetryProxy
    {
        return new DbRetryProxy($this->logger, $maxTries, [PDOException::class]);
    }
}
