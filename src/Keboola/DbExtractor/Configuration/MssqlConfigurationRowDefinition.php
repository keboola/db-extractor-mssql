<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class MssqlConfigurationRowDefinition extends ConfigRowDefinition
{
    public function getConfigTreeBuilder(): TreeBuilder
    {
        $treeBuilder = new TreeBuilder();
        $rootNode = $treeBuilder->root('parameters');
        // @formatter:off
        $rootNode
            ->validate()->always(function ($v) {
                if (isset($v['disableFallback'])
                    && $v['disableFallback'] === true
                    && isset($v['disableBcp'])
                    && $v['disableBcp'] === true
                ) {
                    throw new InvalidConfigurationException('Can\'t disable both BCP and fallback to PDO');
                }
                return $v;
            })->end()
            ->children()
                ->scalarNode('data_dir')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('extractor_class')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->arrayNode('db')
                    ->children()
                        ->scalarNode('driver')->end()
                        ->scalarNode('host')->end()
                        ->scalarNode('port')->end()
                        ->scalarNode('database')->end()
                        ->scalarNode('user')
                            ->isRequired()
                        ->end()
                        ->scalarNode('#password')->end()
                        ->append($this->addSshNode())
                    ->end()
                ->end()
                ->integerNode('id')
                ->end()
                ->scalarNode('name')
                ->end()
                ->scalarNode('query')->end()
                ->arrayNode('table')
                    ->children()
                        ->scalarNode('schema')->end()
                        ->scalarNode('tableName')->end()
                    ->end()
                ->end()
                ->arrayNode('columns')
                    ->prototype('scalar')->end()
                ->end()
                ->scalarNode('outputTable')
                    ->isRequired()
                ->end()
                ->booleanNode('incremental')
                    ->defaultValue(false)
                ->end()
                ->scalarNode('incrementalFetchingColumn')->end()
                ->scalarNode('incrementalFetchingLimit')->end()
                ->booleanNode('enabled')
                    ->defaultValue(true)
                ->end()
                ->arrayNode('primaryKey')
                    ->prototype('scalar')->end()
                ->end()
                ->integerNode('retries')
                    ->min(1)
                ->end()
                ->booleanNode('nolock')->defaultValue(false)->end()
                ->booleanNode('advancedMode')->end()
                ->booleanNode('disableFallback')->defaultFalse()->end()
                ->booleanNode('disableBcp')->defaultFalse()->end()
            ->end()
        ;
        // @formatter:on
        return $treeBuilder;
    }
}
