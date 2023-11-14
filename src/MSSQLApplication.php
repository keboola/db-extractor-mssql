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
    protected function loadConfig(): void
    {
        $config = $this->getRawConfig();
        $action = $config['action'] ?? 'run';

        $config['parameters']['data_dir'] = $this->getDataDir();
        $config['parameters']['extractor_class'] = 'MSSQL';

        if ($this->isRowConfiguration($config)) {
            if ($action === 'run') {
                $this->config = new Config(
                    $config,
                    new ConfigRowDefinition(
                        null,
                        null,
                        new MssqlSslNode(),
                        new MssqlTableNodesDecorator(),
                    ),
                );
            } else {
                $this->config = new Config(
                    $config,
                    new ActionConfigRowDefinition(null, null, new MssqlSslNode()),
                );
            }
        } else {
            $this->config = new Config(
                $config,
                new ConfigDefinition(
                    null,
                    null,
                    new MssqlSslNode(),
                    new MssqlTableNodesDecorator(),
                ),
            );
        }
    }


    protected function createExportConfig(array $data): ExportConfig
    {
        return MssqlExportConfig::fromArray($data);
    }
}
