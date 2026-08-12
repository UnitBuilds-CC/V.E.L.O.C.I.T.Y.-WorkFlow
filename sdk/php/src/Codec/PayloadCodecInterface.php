<?php

declare(strict_types=1);

namespace Velocity\SDK\Codec;

/**
 * Interface for payload encoding and decoding.
 *
 * Implementations must handle serialization of arbitrary data to bytes
 * and deserialization from bytes back to the original data format.
 *
 * Built-in implementations:
 * - JsonPayloadCodec: JSON encoding
 * - BinaryPayloadCodec: raw byte passthrough
 */
interface PayloadCodecInterface
{
    /**
     * Encode data to bytes.
     *
     * @param mixed $data The data to encode
     * @return string Raw bytes (binary string)
     * @throws CodecException If encoding fails
     */
    public function encode(mixed $data): string;

    /**
     * Decode bytes to data.
     *
     * @param string $data Raw bytes to decode
     * @param string|null $type Optional target type hint
     * @return mixed Decoded data
     * @throws CodecException If decoding fails
     */
    public function decode(string $data, ?string $type = null): mixed;
}

/**
 * Exception thrown when codec operations fail.
 */
class CodecException extends \RuntimeException
{
    public static function encodeFailed(string $reason, ?\Throwable $previous = null): self
    {
        return new self("Encoding failed: {$reason}", 0, $previous);
    }

    public static function decodeFailed(string $reason, ?\Throwable $previous = null): self
    {
        return new self("Decoding failed: {$reason}", 0, $previous);
    }
}
