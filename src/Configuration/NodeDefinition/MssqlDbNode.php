<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration\NodeDefinition;

use Keboola\DbExtractorConfig\Configuration\NodeDefinition\DbNode;
use Symfony\Component\Config\Definition\Builder\NodeBuilder;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class MssqlDbNode extends DbNode
{
    protected function init(NodeBuilder $builder): void
    {
        parent::init($builder);
        $this->addInstanceNode($builder);
        $this->addQueryTimeoutNode($builder);
        $this->addServicePrincipalNodes($builder);
        $this->addAuthValidation();
    }

    protected function addUserNode(NodeBuilder $builder): void
    {
        // Optional: not required when Service Principal credentials are provided.
        $builder->scalarNode('user');
    }

    protected function addPasswordNode(NodeBuilder $builder): void
    {
        // Optional: not required when Service Principal credentials are provided.
        $builder->scalarNode('#password');
    }

    protected function addInstanceNode(NodeBuilder $builder): void
    {
        $builder->scalarNode('instance');
    }

    protected function addQueryTimeoutNode(NodeBuilder $builder): void
    {
        $builder->integerNode('queryTimeout');
    }

    protected function addServicePrincipalNodes(NodeBuilder $builder): void
    {
        // Azure AD Service Principal credentials
        $builder->scalarNode('tenantId');
        $builder->scalarNode('clientId');
        $builder->scalarNode('#clientSecret');
    }

    protected function addAuthValidation(): void
    {
        $this->validate()->always(function (array $v): array {
            $spKeys = ['tenantId', 'clientId', '#clientSecret'];
            $spFilled = count(array_filter($spKeys, fn(string $k): bool => !empty($v[$k])));

            if ($spFilled > 0 && $spFilled < count($spKeys)) {
                throw new InvalidConfigurationException(
                    'For Service Principal authentication all of "tenantId", "clientId" and '
                    . '"#clientSecret" must be set.',
                );
            }

            if ($spFilled === 0 && (empty($v['user']) || empty($v['#password']))) {
                throw new InvalidConfigurationException(
                    'Either "user" and "#password" (SQL login) or "tenantId", "clientId" and '
                    . '"#clientSecret" (Service Principal) must be set.',
                );
            }

            return $v;
        });
    }
}
