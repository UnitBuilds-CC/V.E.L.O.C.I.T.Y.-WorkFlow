<?php

declare(strict_types=1);

namespace Velocity\SDK\Codec;

/**
 * JSON payload codec implementation.
 *
 * Uses PHP's built-in json_encode/json_decode for serialization.
 *
 * Usage:
 *     $codec = new JsonPayloadCodec();
 *     $encoded = $codec->encode(['key' => 'value']);
 *     $decoded = $codec->decode($encoded);
 */
class JsonPayloadCodec implements PayloadCodecInterface
{
    private int $encodeOptions;
    private int $decodeDepth;

    /**
     * @param int $encodeOptions json_encode options (default: JSON_THROW_ON_ERROR)
     * @param int $decodeDepth   json_decode max depth
     */
    public function __construct(int $encodeOptions = 0, int $decodeDepth = 512)
    {
        $this->encodeOptions = $encodeOptions | JSON_THROW_ON_ERROR;
        $this->decodeDepth = $decodeDepth;
    }

    /**
     * Encode data to JSON bytes.
     *
     * @param mixed $data The data to encode
     * @return string UTF-8 encoded JSON string
     */
    public function encode(mixed $data): string
    {
        if ($data === null) {
            return '';
        }

        if (is_string($data)) {
            return $data;
        }

        try {
            return json_encode($data, $this->encodeOptions);
        } catch (\JsonException $e) {
            throw CodecException::encodeFailed($e->getMessage(), $e);
        }
    }

    /**
     * Decode JSON bytes to data.
     *
     * @param string $data Raw bytes to decode
     * @param string|null $type Optional target type (not used, returns associative arrays)
     * @return mixed Decoded data
     */
    public function decode(string $data, ?string $type = null): mixed
    {
        if ($data === '') {
            return null;
        }

        try {
            return json_decode($data, true, $this->decodeDepth, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw CodecException::decodeFailed($e->getMessage(), $e);
        }
    }
}

/**
 * Binary payload codec (passthrough).
 */
class BinaryPayloadCodec implements PayloadCodecInterface
{
    public function encode(mixed $data): string
    {
        if (!is_string($data)) {
            throw CodecException::encodeFailed(
                'BinaryPayloadCodec expects string, got ' . gettype($data)
            );
        }
        return $data;
    }

    public function decode(string $data, ?string $type = null): string
    {
        return $data;
    }
}

/**
 * Null codec — encodes everything as empty string.
 */
class NullPayloadCodec implements PayloadCodecInterface
{
    public function encode(mixed $data): string
    {
        return '';
    }

    public function decode(string $data, ?string $type = null): null
    {
        return null;
    }
}
