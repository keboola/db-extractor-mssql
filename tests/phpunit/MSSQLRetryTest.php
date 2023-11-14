<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Extractor\MssqlRetryFactory;
use PDOException;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;
use RuntimeException;

class MSSQLRetryTest extends TestCase
{
    public function testNoRetry(): void
    {
        $logger = new TestLogger();
        $proxy = MssqlRetryFactory::createProxy($logger, 3);

        try {
            $proxy->call(function (): void {
                // Runtime exception is not in default expected exception's classse
                throw new RuntimeException('Error!');
            });
            $this->fail('Expected RuntimeException.');
        } catch (RuntimeException $e) {
            // ok
        }

        $this->assertCount(0, $logger->records);
    }

    public function testSimpleRetry(): void
    {
        $logger = new TestLogger();
        $timeout = 15 * 60 * 1000; // not used, 15 min
        $maxTries = 4;
        $proxy = MssqlRetryFactory::createProxy($logger, $maxTries, $timeout, [PDOException::class]);

        $realTries = 0;
        try {
            $proxy->call(function () use (&$realTries): void {
                $realTries++;
                throw new PDOException('Error!');
            });
            $this->fail('Expected PdoException.');
        } catch (PDOException $e) {
            // ok
        }

        $this->assertSame($realTries, $maxTries);
        $this->assertTrue($logger->hasInfoThatContains('Error!. Retrying... [1x]'));
        $this->assertTrue($logger->hasInfoThatContains('Error!. Retrying... [2x]'));
        $this->assertTrue($logger->hasInfoThatContains('Error!. Retrying... [3x]'));
        $this->assertFalse($logger->hasInfoThatContains('Error!. Retrying... [4x]'));
    }

    public function testTimeoutRetryForProblematicError(): void
    {
        $logger = new TestLogger();
        $timeout = 5 * 1000; // 5s
        $maxTries = 1;
        $proxy = MssqlRetryFactory::createProxy($logger, $maxTries, $timeout, [PDOException::class]);

        $realTries = 0;
        $start = microtime(true);
        try {
            $proxy->call(function () use (&$realTries): void {
                $realTries++;
                throw new PDOException(
                    'Client unable to establish connection because ' .
                    'an error was encountered during handshakes before login.',
                );
            });
            $this->fail('Expected RuntimeException.');
        } catch (PDOException $e) {
            // ok
        }
        $end = microtime(true);
        $durationMs = ($end - $start) * 1000;

        // Proxy method is called 3x and retry takes 7 seconds (1s+2s+4s)
        //     exception
        //     [0s < 5s timeout] -> sleep 1s -> [1s < 5s timeout] -> retry
        //     exception
        //     [1s < 5s timeout] -> sleep 2s -> [3s < 5s timeout] -> retry
        //     exception
        //     [3s < 5s timeout] -> sleep 4s -> [7s > 5s timeout] -> NOT retry
        $this->assertSame(3, $realTries);
        $this->assertCount(3, $logger->records);
        $this->assertGreaterThanOrEqual(7000, $durationMs);
        $this->assertLessThan(8000, $durationMs);
    }
}
