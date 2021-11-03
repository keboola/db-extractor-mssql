<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration\NodeDefinition;

use Keboola\DbExtractorConfig\Configuration\NodeDefinition\SslNode;
use Symfony\Component\Config\Definition\Builder\NodeBuilder;

class MssqlSslNode extends SslNode
{
    protected function addCertAndKeyNode(NodeBuilder $nodeBuilder): void
    {
        // not supported
    }

    protected function addCipherNode(NodeBuilder $nodeBuilder): void
    {
        // not supported
    }
}
