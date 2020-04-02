<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

use Keboola\DbExtractorConfig\Configuration\ConfigRowDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\NodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class MssqlConfigRowDefinition extends ConfigRowDefinition
{
    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $treeBuilder = new TreeBuilder('parameters');

        /** @var ArrayNodeDefinition $rootNode */
        $rootNode = $treeBuilder->getRootNode();
        $this->addValidation($rootNode);

        // @formatter:off
        $rootNode
            ->children()
                ->scalarNode('data_dir')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('extractor_class')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->append($this->dbNodeDefinition)
                ->integerNode('id')
                    ->min(0)
                ->end()
                ->scalarNode('name')
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('query')->end()
                ->arrayNode('table')
                    ->children()
                        ->scalarNode('schema')->isRequired()->end()
                        ->scalarNode('tableName')->isRequired()->end()
                        ->booleanNode('changeTracking')->defaultValue(false)->end()
                    ->end()
                ->end()
                ->arrayNode('columns')
                    ->prototype('scalar')->end()
                ->end()
                ->scalarNode('outputTable')
                    ->isRequired()
                    ->cannotBeEmpty()
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
                    ->min(0)
                ->end()
                ->booleanNode('nolock')->defaultValue(false)->end()
                ->booleanNode('advancedMode')->end()
                ->booleanNode('disableFallback')->defaultFalse()->end()
                ->booleanNode('disableBcp')->defaultFalse()->end()
            ->end();
        // @formatter:on

        return $rootNode;
    }

    protected function addValidation(NodeDefinition $definition): NodeDefinition
    {
        $definition
            ->validate()
            ->always(function ($v) {
                if (isset($v['disableFallback'])
                    && $v['disableFallback'] === true
                    && isset($v['disableBcp'])
                    && $v['disableBcp'] === true
                ) {
                    throw new InvalidConfigurationException('Can\'t disable both BCP and fallback to PDO');
                }
                if (isset($v['query']) && $v['query'] !== '' && isset($v['table'])) {
                    throw new InvalidConfigurationException('Both table and query cannot be set together.');
                }
                if (isset($v['query']) && $v['query'] !== '' && isset($v['incrementalFetchingColumn'])) {
                    $message = 'Incremental fetching is not supported for advanced queries.';
                    throw new InvalidConfigurationException($message);
                }
                if (!isset($v['table']) && !isset($v['query'])) {
                    throw new InvalidConfigurationException('One of table or query is required');
                }
                return $v;
            })
            ->end()
        ;
        return $definition;
    }
}
