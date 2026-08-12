package io.velocity.sdk.codec;

import java.nio.charset.StandardCharsets;

/**
 * JSON payload codec implementation.
 *
 * <p>Uses a simple JSON serialization approach. In production, plug in
 * Gson, Jackson, or Moshi by overriding the serialize/deserialize methods.
 *
 * <p>Usage:
 * <pre>{@code
 * PayloadCodec codec = new JsonPayloadCodec();
 * byte[] encoded = codec.encode(Map.of("key", "value"));
 * Map result = codec.decode(encoded, Map.class);
 * }</pre>
 */
public class JsonPayloadCodec implements PayloadCodec {

    /**
     * Encode an object to JSON bytes.
     *
     * <p>This default implementation handles String and byte[] directly.
     * For complex objects, override this method with a proper JSON library.
     *
     * @param data the object to encode
     * @return UTF-8 encoded JSON bytes
     */
    @Override
    public byte[] encode(Object data) throws CodecException {
        if (data == null) {
            return new byte[0];
        }
        if (data instanceof byte[]) {
            return (byte[]) data;
        }
        if (data instanceof String) {
            return ((String) data).getBytes(StandardCharsets.UTF_8);
        }

        // Default: toString() — replace with Gson/Jackson in production
        try {
            String json = toJson(data);
            return json.getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new CodecException("Failed to encode object to JSON", e);
        }
    }

    /**
     * Decode JSON bytes to an object.
     *
     * <p>This default implementation returns the raw string or bytes.
     * For complex deserialization, override with a proper JSON library.
     *
     * @param data the byte array to decode
     * @param type the target class
     * @param <T>  the target type
     * @return decoded object
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T decode(byte[] data, Class<T> type) throws CodecException {
        if (data == null || data.length == 0) {
            return null;
        }

        try {
            if (type == byte[].class) {
                return (T) data;
            }
            if (type == String.class) {
                return (T) new String(data, StandardCharsets.UTF_8);
            }

            // Default: fromJson — replace with Gson/Jackson in production
            String json = new String(data, StandardCharsets.UTF_8);
            return fromJson(json, type);
        } catch (Exception e) {
            throw new CodecException("Failed to decode JSON to object", e);
        }
    }

    /**
     * Serialize an object to JSON string.
     * Override this to use a real JSON library (Gson, Jackson, etc.).
     */
    protected String toJson(Object obj) {
        return obj.toString();
    }

    /**
     * Deserialize a JSON string to an object.
     * Override this to use a real JSON library (Gson, Jackson, etc.).
     */
    protected <T> T fromJson(String json, Class<T> type) {
        throw new UnsupportedOperationException(
            "Override fromJson() with a JSON library (Gson/Jackson) for complex type deserialization"
        );
    }
}
