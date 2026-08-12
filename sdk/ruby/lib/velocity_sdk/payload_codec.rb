# frozen_string_literal: true

require 'json'

module VelocitySdk
  # Interface for payload encoding and decoding.
  #
  # Implementations must handle serialization of arbitrary data to bytes
  # and deserialization from bytes back to the original data format.
  module PayloadCodec
    # Encode data to bytes.
    # @param data [Object] The data to encode
    # @return [String] Encoded bytes (binary string)
    # @raise [CodecError] if encoding fails
    def encode(data)
      raise NotImplementedError
    end

    # Decode bytes to data.
    # @param data [String] Raw bytes to decode
    # @return [Object] Decoded data
    # @raise [CodecError] if decoding fails
    def decode(data)
      raise NotImplementedError
    end
  end

  # Error raised when codec operations fail.
  class CodecError < StandardError; end

  # JSON payload codec.
  #
  # Uses Ruby's built-in JSON library for serialization.
  class JsonCodec
    include PayloadCodec

    def encode(data)
      return '' if data.nil?
      return data if data.is_a?(String)

      JSON.generate(data)
    rescue JSON::GeneratorError => e
      raise CodecError, "JSON encode failed: #{e.message}"
    end

    def decode(data)
      return nil if data.nil? || data.empty?

      JSON.parse(data)
    rescue JSON::ParserError => e
      raise CodecError, "JSON decode failed: #{e.message}"
    end
  end

  # Binary payload codec (passthrough).
  class BinaryCodec
    include PayloadCodec

    def encode(data)
      unless data.is_a?(String)
        raise CodecError, "BinaryCodec expects String, got #{data.class}"
      end
      data.b
    end

    def decode(data)
      data.b
    end
  end

  # Null codec — encodes everything as empty string.
  class NullCodec
    include PayloadCodec

    def encode(_data)
      ''
    end

    def decode(_data)
      nil
    end
  end

  # Chain multiple codecs together.
  #
  # Encoding applies codecs left-to-right; decoding applies right-to-left.
  class CodecChain
    include PayloadCodec

    # @param codecs [Array<PayloadCodec>] Codecs to chain
    def initialize(*codecs)
      raise ArgumentError, 'CodecChain requires at least one codec' if codecs.empty?
      @codecs = codecs
    end

    def encode(data)
      result = data
      @codecs.each do |codec|
        result = codec.encode(result)
      end
      result
    end

    def decode(data)
      result = data
      @codecs.reverse.each do |codec|
        result = codec.decode(result)
      end
      result
    end
  end
end
