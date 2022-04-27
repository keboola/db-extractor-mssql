<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Exception;

use Throwable;

class BcpAdapterException extends ApplicationException
{
    public function __construct(string $message = '', int $code = 0, ?Throwable $previous = null, array $data = [])
    {
        if (isset($data['currentLine'])) {
            $message .= sprintf(' Error on line "%s"', implode(';', $data['currentLine']));
            if (isset($data['currentLineNumber'])) {
                $message .= sprintf(' [%s].', $data['currentLineNumber']);
            }
        }

        if (isset($data['bcpErrorOutput'])) {
            $message .= sprintf(' BCP Error "%s".', $data['bcpErrorOutput']);
        }
        parent::__construct($message, $code, $previous, $data);
    }
}
