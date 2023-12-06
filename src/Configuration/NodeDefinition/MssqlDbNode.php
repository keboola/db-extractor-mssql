<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration\NodeDefinition;

use Keboola\DbExtractorConfig\Configuration\NodeDefinition\DbNode;
use Symfony\Component\Config\Definition\Builder\NodeBuilder;

class MssqlDbNode extends DbNode
{
    protected function init(NodeBuilder $builder): void
    {
        parent::init($builder);
        $this->addInstanceNode($builder);
    }

    protected function addInstanceNode(NodeBuilder $builder): void
    {
        $builder->scalarNode('instance');
    }
}
