<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Configuration\MssqlConfigurationRowDefinition;
use Keboola\DbExtractor\Configuration\MssqlConfigurationDefinition;
use Keboola\DbExtractorLogger\Logger;

class MSSQLApplication extends Application
{
    public function __construct(array $config, ?Logger $logger = null, array $state = [], string $dataDir = '/data/')
    {
        $config['parameters']['data_dir'] = $dataDir;
        $config['parameters']['extractor_class'] = 'MSSQL';

        parent::__construct($config, ($logger) ? $logger : new Logger("ex-db-mssql"), $state);

        // override with mssql specific config definitions
        if (isset($this['parameters']['tables'])) {
            $this->setConfigDefinition(new MssqlConfigurationDefinition());
        } else if ($this['action'] === 'run') {
            $this->setConfigDefinition(new MssqlConfigurationRowDefinition());
        }
    }
}
