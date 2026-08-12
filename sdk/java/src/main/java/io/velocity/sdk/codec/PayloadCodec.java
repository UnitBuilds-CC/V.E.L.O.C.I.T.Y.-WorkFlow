package io.velocity.sdk.codec;

/**
 * Interface for payload encoding and decoding.
 *
 * <p>Implementations must handle serialization of arbitrary objects to bytes
 * and deserialization from bytes back to the original data format.
 *
 * <p>Built-in implementations:
 * <ul>
 *   <li>{@link JsonPayloadCodec} — JSON encoding via Gson/Jackson</li>
 *   <li>{@link BinaryPayloadCodec} — raw byte passthrough</li>
 * </ul>
 */
public interface PayloadCodec {

    /**
     * Encode an object to bytes.
     *
     * @param data the object to encode
     * @return encoded byte array
     * @throws CodecException if encoding fails
     */
    byte[] encode(Object data) throws CodecException;

    /**
     * Decode bytes to an object.
     *
     * @param data the byte array to decode
     * @param type the target class
     * @param <T>  the target type
     * @return decoded object
     * @throws CodecException if decoding fails
     */
    <T> T decode(byte[] data, Class<T> type) throws CodecException;

    /**
     * Exception thrown when codec operations fail.
     */
    class CodecException extends RuntimeException {
        public CodecException(String message) {
            super(message);
        }

        public CodecException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
