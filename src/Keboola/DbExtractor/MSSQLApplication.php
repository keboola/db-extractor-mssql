<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Configuration\MSSSQLConfigDefinition;

class MSSQLApplication extends Application
{
    public function __construct(array $config, string $dataDir)
    {
        $config['parameters']['data_dir'] = $dataDir;
        $config['parameters']['extractor_class'] = 'MSSQL';

        parent::__construct($config);

        $this->setConfigDefinition(new MSSSQLConfigDefinition());
    }
}
