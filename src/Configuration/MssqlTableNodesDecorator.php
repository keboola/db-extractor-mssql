<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

use Keboola\DbExtractorConfig\Configuration\NodeDefinition\TableNodesDecorator;
use Symfony\Component\Config\Definition\Builder\NodeBuilder;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class MssqlTableNodesDecorator extends TableNodesDecorator
{
    public function validate(array $v): array
    {
        $v = parent::validate($v);
        $disableBcp = $v['disableBcp'] ?? false;
        $disableFallback = $v['disableFallback']  ?? false;

        if ($disableBcp && $disableFallback) {
            throw new InvalidConfigurationException('Can\'t disable both BCP and fallback to PDO');
        }

        return $v;
    }

    public function addNodes(NodeBuilder $builder): void
    {
        parent::addNodes($builder);
        $this->addAdvancedModeNode($builder);
        $this->addNoLockNode($builder);
        $this->addDisableBcpNode($builder);
        $this->addDisableFallbackNode($builder);
    }

    protected function addAdvancedModeNode(NodeBuilder $builder): void
    {
        //Backwards compatibility with old configurations. Not used
        $builder->booleanNode('advancedMode')->end();
    }

    protected function addNoLockNode(NodeBuilder $builder): void
    {
        $builder->booleanNode('nolock')->defaultValue(false);
    }

    protected function addDisableBcpNode(NodeBuilder $builder): void
    {
        $builder->booleanNode('disableBcp')->defaultFalse();
    }

    protected function addDisableFallbackNode(NodeBuilder $builder): void
    {
        $builder->booleanNode('disableFallback')->defaultFalse();
    }
}
