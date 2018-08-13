<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Configuration\MSSSQLConfigDefinition;

class MSSQLApplication extends Application
{
    public function __construct(array $config, ?Logger $logger = null, array $state = [], string $dataDir = '/data/')
    {
        $config['parameters']['data_dir'] = $dataDir;
        $config['parameters']['extractor_class'] = 'MSSQL';

        parent::__construct($config, ($logger) ? $logger : new Logger("ex-db-mssql"), $state);

        $this->setConfigDefinition(new MSSSQLConfigDefinition());
    }
}
