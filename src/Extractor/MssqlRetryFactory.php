<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Psr\Log\LoggerInterface;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\CallableRetryPolicy;
use Retry\RetryProxy;
use Throwable;

class MssqlRetryFactory
{
    public static function createProxy(
        LoggerInterface $logger,
        int $maxTries,
        ?int $timeout = null,
        ?array $expectedExceptions = null,
    ): RetryProxy {
        $timeout = $timeout ?? 15 * 60 * 1000; // default timeout, 15 min
        $expectedExceptions = $expectedExceptions ?? ['PDOException', 'ErrorException'];

        // Retry inverval 1s -> 2s -> 4s -> ... 60s
        $backOffPolicy = new ExponentialBackOffPolicy(1000, 2, 60000);

        // Retry policy
        // We use by default "simple retry", see $maxTries
        // For problematic error we use "timeout retry", see $timeout (more time is needed)
        $exceptions = [];
        $start = microtime(true);
        $canRetry = function (Throwable $e) use (&$exceptions, $start, $maxTries, $expectedExceptions, $timeout) {
            $ms = (microtime(true) - $start) * 1000;

            // canRetry callback is called multiple times, so we count each unique exception
            if (!in_array($e, $exceptions, true)) {
                $exceptions[] = $e;
            }

            // Simple retry by default
            if (count($exceptions) < $maxTries && self::shouldRetryForException($e, $expectedExceptions)) {
                return true;
            }

            // Timeout retry for problematic error "error was encountered during handshakes before login ..."
            $longRetryMsg = 'error was encountered during handshakes before login.';
            if ($ms <= $timeout && strpos($e->getMessage(), $longRetryMsg) !== false) {
                return true;
            }

            return false;
        };
        $retryPolicy = new CallableRetryPolicy($canRetry, 30);

        return new RetryProxy($retryPolicy, $backOffPolicy, $logger);
    }

    private static function shouldRetryForException(Throwable $e, array $expectedExceptions): bool
    {
        foreach ($expectedExceptions as $class) {
            if (is_a($e, $class)) {
                return true;
            }
        }

        return false;
    }
}
