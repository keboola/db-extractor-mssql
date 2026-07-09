<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Exception\UserException;

/**
 * Mints an Azure AD access token for a Service Principal using the OAuth2
 * client_credentials flow and stores it in a file consumable by BCP (`-G -P <file>`).
 */
class ServicePrincipalTokenProvider
{
    private const LOGIN_URL = 'https://login.microsoftonline.com/%s/oauth2/v2.0/token';

    private const SCOPE = 'https://database.windows.net/.default';

    private string $tenantId;

    private string $clientId;

    private string $clientSecret;

    public function __construct(string $tenantId, string $clientId, string $clientSecret)
    {
        $this->tenantId = $tenantId;
        $this->clientId = $clientId;
        $this->clientSecret = $clientSecret;
    }

    public function getAccessToken(): string
    {
        $response = $this->sendTokenRequest(
            sprintf(self::LOGIN_URL, $this->tenantId),
            [
                'grant_type' => 'client_credentials',
                'client_id' => $this->clientId,
                'client_secret' => $this->clientSecret,
                'scope' => self::SCOPE,
            ],
        );

        /** @var array<string, mixed>|null $data */
        $data = json_decode($response, true);
        if (!is_array($data) || !isset($data['access_token']) || !is_string($data['access_token'])) {
            throw new UserException('Azure AD token response did not contain an access token.');
        }

        return $data['access_token'];
    }

    /**
     * Writes the access token to a locked-down (0600) temp file re-encoded as
     * UTF-16LE without BOM, which is the format BCP expects for `-P <tokenfile>`.
     */
    public static function createTokenFile(string $token): string
    {
        $filePath = (string) tempnam(sys_get_temp_dir(), 'mssql-sp-token-');
        chmod($filePath, 0600);

        $encoded = mb_convert_encoding(rtrim($token, "\r\n"), 'UTF-16LE', 'UTF-8');
        file_put_contents($filePath, $encoded);

        return $filePath;
    }

    /**
     * @param array<string, string> $body
     */
    protected function sendTokenRequest(string $url, array $body): string
    {
        $ch = curl_init($url);
        if ($ch === false) {
            throw new UserException('Unable to initialize the Azure AD token request.');
        }

        curl_setopt($ch, CURLOPT_POST, true);
        curl_setopt($ch, CURLOPT_POSTFIELDS, http_build_query($body));
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_HTTPHEADER, ['Content-Type: application/x-www-form-urlencoded']);

        $response = curl_exec($ch);
        $statusCode = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $error = curl_error($ch);
        curl_close($ch);

        if ($response === false) {
            throw new UserException(sprintf('Failed to request Azure AD token: %s', $error));
        }

        if ($statusCode < 200 || $statusCode >= 300) {
            throw new UserException(sprintf(
                'Failed to obtain Azure AD token (HTTP %d): %s',
                $statusCode,
                (string) $response,
            ));
        }

        return (string) $response;
    }
}
