<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Configuration\MssqlExportConfig;
use Keboola\DbExtractor\Configuration\MssqlTableNodesDecorator;
use Keboola\DbExtractor\Configuration\NodeDefinition\MssqlSslNode;
use Keboola\DbExtractorConfig\Config;
use Keboola\DbExtractorConfig\Configuration\ActionConfigRowDefinition;
use Keboola\DbExtractorConfig\Configuration\ConfigDefinition;
use Keboola\DbExtractorConfig\Configuration\ConfigRowDefinition;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Psr\Log\LoggerInterface;

class MSSQLApplication extends Application
{
    public function __construct(array $config, LoggerInterface $logger, array $state, string $dataDir)
    {
        $config['parameters']['data_dir'] = $dataDir;
        $config['parameters']['extractor_class'] = 'MSSQL';

        parent::__construct($config, $logger, $state);
    }

    protected function buildConfig(array $config): void
    {
        if ($this->isRowConfiguration($config)) {
            if ($this['action'] === 'run') {
                $this->config = new Config(
                    $config,
                    new ConfigRowDefinition(
                        null,
                        null,
                        new MssqlSslNode(),
                        new MssqlTableNodesDecorator()
                    )
                );
            } else {
                $this->config = new Config(
                    $config,
                    new ActionConfigRowDefinition(null, null, new MssqlSslNode())
                );
            }
        } else {
            $this->config = new Config(
                $config,
                new ConfigDefinition(
                    null,
                    null,
                    new MssqlSslNode(),
                    new MssqlTableNodesDecorator()
                )
            );
        }
    }

    protected function createExportConfig(array $data): ExportConfig
    {
        return MssqlExportConfig::fromArray($data);
    }
}
