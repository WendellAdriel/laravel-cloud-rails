# frozen_string_literal: true

require "base64"
require "json"
require "openssl"

module LaravelCloud
  module Queue
    # A port of Laravel's Illuminate\Encryption\Encrypter, for reading what
    # Laravel Cloud encrypts with the application key (APP_KEY): a failed job
    # handed back for retry.
    #
    # The format is base64(JSON{iv, value, mac, tag}): AES-CBC authenticated
    # with an HMAC-SHA256 "mac", or AES-GCM authenticated with its "tag". The
    # mode is read from the payload (a tag means GCM) and the key size from the
    # key, so no cipher needs configuring.
    class LaravelEncrypter
      class DecryptError < LaravelCloud::Error; end

      KEY_SIZES = [16, 32].freeze

      # `key` is the raw key or an APP_KEY-style "base64:..." value.
      def initialize(key)
        key = key.to_s
        @key = key.start_with?("base64:") ? Base64.strict_decode64(key.delete_prefix("base64:")) : key

        raise ArgumentError, "The application key must be 16 or 32 bytes." unless KEY_SIZES.include?(@key.bytesize)
      end

      # The cipher this key implies, spelled as Laravel's `app.cipher` — what
      # Laravel sends Cloud in the Cloud-Encryption-Cipher header. Laravel's
      # default is AES-256-CBC, which a 32-byte key (any `php artisan
      # key:generate` key) gives.
      def cipher
        "AES-#{@key.bytesize * 8}-CBC"
      end

      # Encrypter::encryptString. (Laravel Cloud does the encrypting in
      # practice; this exists so tests can produce real payloads.)
      def encrypt_string(value, gcm: false)
        cipher = OpenSSL::Cipher.new(cipher_name(gcm)).encrypt
        cipher.key = @key
        iv = cipher.random_iv
        encrypted = cipher.update(value.to_s) + cipher.final

        iv64 = Base64.strict_encode64(iv)
        value64 = Base64.strict_encode64(encrypted)

        Base64.strict_encode64(JSON.generate(
          "iv" => iv64,
          "value" => value64,
          "mac" => gcm ? "" : mac(iv64, value64),
          "tag" => gcm ? Base64.strict_encode64(cipher.auth_tag) : "",
        ))
      end

      # Encrypter::decryptString.
      def decrypt_string(payload)
        data = parse(payload)
        tag = data["tag"].to_s.empty? ? nil : Base64.strict_decode64(data["tag"])
        gcm = !tag.nil?

        if gcm
          raise DecryptError, "Could not decrypt the data." unless tag.bytesize == 16
        elsif !OpenSSL.secure_compare(mac(data["iv"], data["value"]), data["mac"])
          raise DecryptError, "The MAC is invalid."
        end

        cipher = OpenSSL::Cipher.new(cipher_name(gcm)).decrypt
        cipher.key = @key
        cipher.iv = Base64.strict_decode64(data["iv"])
        cipher.auth_tag = tag if gcm

        cipher.update(Base64.strict_decode64(data["value"])) + cipher.final
      rescue OpenSSL::Cipher::CipherError, ArgumentError
        raise DecryptError, "Could not decrypt the data."
      end

      private

      def cipher_name(gcm)
        "aes-#{@key.bytesize * 8}-#{gcm ? 'gcm' : 'cbc'}"
      end

      def mac(iv64, value64)
        OpenSSL::HMAC.hexdigest("SHA256", @key, iv64 + value64)
      end

      def parse(payload)
        data = JSON.parse(Base64.decode64(payload.to_s))

        unless data.is_a?(Hash) && %w[iv value mac].all? { |key| data[key].is_a?(String) }
          raise DecryptError, "The payload is invalid."
        end

        data
      rescue JSON::ParserError
        raise DecryptError, "The payload is invalid."
      end
    end
  end
end
