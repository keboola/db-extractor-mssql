<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Extractor\NullAwareCsvWriter;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class NullAwareCsvWriterTest extends TestCase
{
    private string $tmpFile;

    private NullAwareCsvWriter $writer;

    protected function setUp(): void
    {
        parent::setUp();
        $tmpFile = tempnam(sys_get_temp_dir(), 'null-aware-csv-');
        if ($tmpFile === false) {
            Assert::fail('Could not create a temporary file for the test.');
        }
        $this->tmpFile = $tmpFile;
        $this->writer = new NullAwareCsvWriter($this->tmpFile);
    }

    protected function tearDown(): void
    {
        if (file_exists($this->tmpFile)) {
            unlink($this->tmpFile);
        }
        parent::tearDown();
    }

    public function testNullIsWrittenAsUnquotedEmptyField(): void
    {
        // Regression for SUPPORT-16443 / CFTL-677: the default CsvWriter serializes a SQL NULL
        // as a quoted empty string (""), which Snowflake cannot load into a typed DATE/TIMESTAMP
        // column ("Timestamp '' is not recognized"). A NULL must become an UNQUOTED empty field
        // so Snowflake's EMPTY_FIELD_AS_NULL converts it back to NULL on COPY INTO.
        Assert::assertSame(
            "\"1\",,\"2021-01-05 13:43:14.490\"\n",
            $this->writer->rowToStr(['1', null, '2021-01-05 13:43:14.490']),
        );
    }

    public function testEmptyStringStaysQuotedToPreserveNullDistinction(): void
    {
        // An empty string is NOT NULL and must keep its enclosure, otherwise it would also be
        // imported as NULL. This mirrors the BCP adapter's behaviour (unquoted empty for NULL,
        // "" for an empty string), keeping the PDO output consistent with it.
        Assert::assertSame(
            "\"\",,\"x\"\n",
            $this->writer->rowToStr(['', null, 'x']),
        );
    }

    public function testRegularValuesAreEnclosedAndEscaped(): void
    {
        Assert::assertSame(
            "\"a\",\"b\"\"c\",\"1\"\n",
            $this->writer->rowToStr(['a', 'b"c', '1']),
        );
    }

    public function testRowOfOnlyNullsProducesUnquotedEmptyFields(): void
    {
        Assert::assertSame(
            ",,\n",
            $this->writer->rowToStr([null, null, null]),
        );
    }
}
