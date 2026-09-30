<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Extractor\ServicePrincipalTokenProvider;
use PHPUnit\Framework\TestCase;

class ServicePrincipalTokenProviderTest extends TestCase
{
    public function testCreateTokenFileEncodesUtf16LeWithoutBom(): void
    {
        $file = ServicePrincipalTokenProvider::createTokenFile("my-token\n");

        $this->assertFileExists($file);
        // Trailing newline stripped and re-encoded to UTF-16LE (no BOM).
        $this->assertSame(
            mb_convert_encoding('my-token', 'UTF-16LE', 'UTF-8'),
            (string) file_get_contents($file),
        );
        $this->assertSame('0600', substr(sprintf('%o', fileperms($file)), -4));
        @unlink($file);
    }

    public function testGetAccessTokenParsesResponse(): void
    {
        $provider = $this->createProviderWithResponse('{"access_token":"abc123","token_type":"Bearer"}');
        $this->assertSame('abc123', $provider->getAccessToken());
    }

    public function testGetAccessTokenThrowsOnMissingToken(): void
    {
        $provider = $this->createProviderWithResponse('{"error":"invalid_client"}');
        $this->expectException(UserException::class);
        $provider->getAccessToken();
    }

    private function createProviderWithResponse(string $response): ServicePrincipalTokenProvider
    {
        return new class ('tenant', 'client', 'secret', $response) extends ServicePrincipalTokenProvider {
            private string $stubResponse;

            public function __construct(string $tenantId, string $clientId, string $clientSecret, string $response)
            {
                parent::__construct($tenantId, $clientId, $clientSecret);
                $this->stubResponse = $response;
            }

            protected function sendTokenRequest(string $url, array $body): string
            {
                return $this->stubResponse;
            }
        };
    }
}
