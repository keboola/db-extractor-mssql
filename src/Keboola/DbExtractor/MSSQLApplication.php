<?php
/**
 * @package ex-db-mssql
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Configuration\MSSSQLConfigDefinition;

class MSSQLApplication extends Application
{
    public function __construct(array $config, $dataDir)
    {
        $config['parameters']['data_dir'] = $dataDir;
        $config['parameters']['extractor_class'] = 'MSSQL';

        parent::__construct($config);

        $this->setConfigDefinition(new MSSSQLConfigDefinition());
    }
}
