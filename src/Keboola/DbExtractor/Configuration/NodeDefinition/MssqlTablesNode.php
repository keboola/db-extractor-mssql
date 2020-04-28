<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration\NodeDefinition;

use Keboola\DbExtractorConfig\Configuration\NodeDefinition\TablesNode;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class MssqlTablesNode extends TablesNode
{
    protected function init(): void
    {
        // @formatter:off
        $this
            ->prototype('array')
            ->validate()->always(function ($v) {
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
            })->end()
            ->children()
                ->integerNode('id')
                    ->isRequired()
                    ->min(0)
                ->end()
                ->scalarNode('name')
                    ->isRequired()
                    ->cannotBeEmpty()
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
                    ->cannotBeEmpty()
                ->end()
                ->booleanNode('incremental')
                    ->defaultValue(false)
                ->end()
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
                ->booleanNode('disableFallback')->defaultFalse()->end()
                ->booleanNode('disableBcp')->defaultFalse()->end()
                ->enumNode('exportMethod')
                    ->values(['auto', 'bcp', 'pdo'])
                    ->defaultValue('auto')
                ->end()
            ->end()
        ;
        // @formatter:on
    }
}
