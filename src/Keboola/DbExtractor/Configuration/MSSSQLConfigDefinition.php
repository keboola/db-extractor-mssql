<?php
/**
 * @package ex-db-mssql
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\DbExtractor\Configuration;

use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class MSSSQLConfigDefinition extends ConfigDefinition
{
    /**
     * Generates the configuration tree builder.
     *
     * @return \Symfony\Component\Config\Definition\Builder\TreeBuilder The tree builder
     */
    public function getConfigTreeBuilder()
    {
        $treeBuilder = new TreeBuilder();
        $rootNode = $treeBuilder->root('config');

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
                ->arrayNode('parameters')
                    ->children()
                        ->arrayNode('db')
                            ->children()
                                ->scalarNode('host')->end()
                                ->scalarNode('port')->end()
                                ->scalarNode('database')
                                    ->isRequired()
                                    ->cannotBeEmpty()
                                ->end()
                                ->scalarNode('user')
                                    ->isRequired()
                                ->end()
                                ->scalarNode('password')
                                    ->isRequired()
                                ->end()
                                ->append($this->addSshNode())
                            ->end()
                        ->end()
                        ->arrayNode('tables')
                            ->isRequired()
                            ->prototype('array')
                                ->children()
                                    ->integerNode('id')
                                        ->isRequired()
                                        ->min(0)
                                    ->end()
                                    ->scalarNode('name')
                                        ->isRequired()
                                        ->cannotBeEmpty()
                                    ->end()
                                    ->scalarNode('query')
                                        ->isRequired()
                                        ->cannotBeEmpty()
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
                                    ->scalarNode('primaryKey')
                                        ->defaultValue(null)
                                    ->end()
                                ->end()
                            ->end()
                        ->end()
                    ->end()
                ->end()
                ->arrayNode('image_parameters')
                ->end()
            ->end()
        ;

        return $treeBuilder;
    }
}
