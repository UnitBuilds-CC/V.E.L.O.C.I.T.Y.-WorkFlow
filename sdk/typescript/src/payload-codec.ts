/**
 * VELOCITY-WorkFlow TypeScript SDK - Payload encoding/decoding.
 *
 * Provides codecs for serializing workflow payloads (JSON, binary).
 *
 * @packageDocumentation
 */

/**
 * Interface for payload encoding/decoding.
 *
 * Implementations must handle serialization of arbitrary data to bytes
 * and deserialization from bytes back to the original data format.
 */
export interface PayloadCodec {
  /** Encode data to a Uint8Array (bytes). */
  encode(data: unknown): Uint8Array;

  /** Decode bytes back to data. */
  decode(data: Uint8Array): unknown;
}

/**
 * JSON payload codec.
 *
 * Serializes data as UTF-8 JSON strings.
 */
export class JsonCodec implements PayloadCodec {
  encode(data: unknown): Uint8Array {
    const json = JSON.stringify(data);
    return new TextEncoder().encode(json);
  }

  decode(data: Uint8Array): unknown {
    const json = new TextDecoder().decode(data);
    return JSON.parse(json);
  }
}

/**
 * Binary payload codec (passthrough).
 *
 * Accepts only Uint8Array input and returns it as-is.
 */
export class BinaryCodec implements PayloadCodec {
  encode(data: unknown): Uint8Array {
    if (!(data instanceof Uint8Array)) {
      throw new TypeError(`BinaryCodec expects Uint8Array, got ${typeof data}`);
    }
    return data;
  }

  decode(data: Uint8Array): Uint8Array {
    return data;
  }
}

/**
 * Null/undefined codec — encodes everything as empty bytes.
 *
 * Useful for workflows that take no input and return no output.
 */
export class NullCodec implements PayloadCodec {
  encode(_data: unknown): Uint8Array {
    return new Uint8Array(0);
  }

  decode(_data: Uint8Array): null {
    return null;
  }
}

/**
 * Chain multiple codecs together (e.g., JSON encode then compress).
 *
 * Encoding applies codecs left-to-right; decoding applies right-to-left.
 */
export class CodecChain implements PayloadCodec {
  private readonly codecs: PayloadCodec[];

  constructor(codecs: PayloadCodec[]) {
    if (codecs.length === 0) {
      throw new Error('CodecChain requires at least one codec');
    }
    this.codecs = codecs;
  }

  encode(data: unknown): Uint8Array {
    let result: unknown = data;
    for (const codec of this.codecs) {
      result = codec.encode(result);
    }
    return result instanceof Uint8Array ? result : this.codecs[this.codecs.length - 1].encode(result);
  }

  decode(data: Uint8Array): unknown {
    let result: unknown = data;
    for (let i = this.codecs.length - 1; i >= 0; i--) {
      result = this.codecs[i].decode(result as Uint8Array);
    }
    return result;
  }
}
