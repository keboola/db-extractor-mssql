<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

use Keboola\DbExtractorConfig\Configuration\ConfigRowDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;

class MssqlConfigRowDefinition extends ConfigRowDefinition
{
    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $rootNode = parent::getParametersDefinition();

        // @formatter:off
        $rootNode
            ->children()
                ->booleanNode('propagateDescriptions')
                    ->defaultTrue();
        // @formatter:on

        return $rootNode;
    }
}
