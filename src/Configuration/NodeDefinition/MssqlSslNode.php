<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration\NodeDefinition;

use Keboola\DbExtractorConfig\Configuration\NodeDefinition\SslNode;
use Symfony\Component\Config\Definition\Builder\NodeBuilder;

class MssqlSslNode extends SslNode
{

    protected function addKeyNode(NodeBuilder $nodeBuilder): void
    {
        // not supported
    }

    protected function addCertNode(NodeBuilder $nodeBuilder): void
    {
        // not supported
    }
}
