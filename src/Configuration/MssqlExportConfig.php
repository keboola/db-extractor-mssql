<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\IncrementalFetchingConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\InputTable;

class MssqlExportConfig extends ExportConfig
{
    private bool $noLock;

    private bool $disableBcp;

    private bool $disableFallback;

    private int $maxTriesBcp;

    public static function fromArray(array $data): self
    {
        return new self(
            $data['id'] ?? null,
            $data['name'] ?? null,
            $data['query'],
            empty($data['query']) ? InputTable::fromArray($data) : null,
            $data['incremental'] ?? false,
            empty($data['query']) ? IncrementalFetchingConfig::fromArray($data) : null,
            $data['columns'],
            $data['outputTable'],
            $data['primaryKey'],
            $data['retries'],
            // Added nodes
            $data['nolock'] ?? false,
            $data['disableBcp'] ?? false,
            $data['disableFallback'] ?? false,
            $data['maxTriesBcp'] ?? 1
        );
    }

    public function __construct(
        ?int $configId,
        ?string $configName,
        ?string $query,
        ?InputTable $table,
        bool $incrementalLoading,
        ?IncrementalFetchingConfig $incrementalFetchingConfig,
        array $columns,
        string $outputTable,
        array $primaryKey,
        int $maxRetries,
        bool $noLock,
        bool $disableBcp,
        bool $disableFallback,
        int $maxTriesBcp
    ) {
        parent::__construct(
            $configId,
            $configName,
            $query,
            $table,
            $incrementalLoading,
            $incrementalFetchingConfig,
            $columns,
            $outputTable,
            $primaryKey,
            $maxRetries
        );
        $this->noLock = $noLock;
        $this->disableBcp = $disableBcp;
        $this->disableFallback = $disableFallback;
        $this->maxTriesBcp = $maxTriesBcp;
    }

    public function getNoLock(): bool
    {
        return $this->noLock;
    }

    public function isBcpDisabled(): bool
    {
        return $this->disableBcp;
    }

    public function isFallbackDisabled(): bool
    {
        return $this->disableFallback;
    }

    public function getMaxTriesBcp(): int
    {
        return $this->maxTriesBcp;
    }
}
