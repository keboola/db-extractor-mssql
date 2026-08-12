<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration\NodeDefinition;

use Keboola\DbExtractor\Configuration\MssqlDatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\NodeDefinition\DbNode;
use Symfony\Component\Config\Definition\Builder\NodeBuilder;

class MssqlDbNode extends DbNode
{
    protected function init(NodeBuilder $builder): void
    {
        parent::init($builder);
        $this->addInstanceNode($builder);
        $this->addQueryTimeoutNode($builder);
        $this->addAuthTypeNode($builder);
        $this->addServicePrincipalNodes($builder);
    }

    protected function addInstanceNode(NodeBuilder $builder): void
    {
        $builder->scalarNode('instance');
    }

    protected function addQueryTimeoutNode(NodeBuilder $builder): void
    {
        $builder->integerNode('queryTimeout');
    }

    /**
     * user / #password are required only for SQL and Entra-password auth; service-principal auth uses
     * clientId / #clientSecret instead. The per-auth-type requiredness is validated in MssqlDatabaseConfig
     * so a clear message is returned. Existing configs (which always send user + #password) are unaffected.
     */
    protected function addUserNode(NodeBuilder $builder): void
    {
        $builder->scalarNode('user');
    }

    protected function addPasswordNode(NodeBuilder $builder): void
    {
        $builder->scalarNode('#password');
    }

    protected function addAuthTypeNode(NodeBuilder $builder): void
    {
        $builder
            ->enumNode('authType')
            ->values([
                MssqlDatabaseConfig::AUTH_TYPE_SQL,
                MssqlDatabaseConfig::AUTH_TYPE_AD_SERVICE_PRINCIPAL,
                MssqlDatabaseConfig::AUTH_TYPE_AD_PASSWORD,
            ]);
    }

    protected function addServicePrincipalNodes(NodeBuilder $builder): void
    {
        $builder->scalarNode('clientId');
        $builder->scalarNode('#clientSecret');
    }
}
