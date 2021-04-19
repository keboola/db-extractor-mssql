<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Exception;

use Keboola\DbExtractor\Adapter\Exception\AdapterSkippedException;

class PdoAdapterSkippedException extends UserException implements AdapterSkippedException
{

}
