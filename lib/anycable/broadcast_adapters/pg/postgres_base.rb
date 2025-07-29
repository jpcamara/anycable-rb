# frozen_string_literal: true

begin
  require "pg"
rescue LoadError
  raise "Please, install the pg gem to use PostgreSQL broadcast adapter"
end

require "json"
require "securerandom"
require "base64"

module AnyCable
  module BroadcastAdapters
    # Base class for PostgreSQL broadcast adapters with large payload support
    class PostgresBase < Base
      attr_reader :channel

      LARGE_PAYLOAD_MARKER = "__large_payload:"
      CLEANUP_FREQUENCY = 100

      def initialize(channel: AnyCable.config.postgres_channel)
        @channel = channel
        @table_created = false
        @cleanup_counter = 0
      end

      def raw_broadcast(payload)
        broadcast_payload = if large_payload?(payload)
          store_large_payload(payload)
        else
          payload
        end

        notify_with_payload(broadcast_payload)
      rescue PG::Error => e
        logger.error "PostgreSQL broadcast failed: #{e.message}"
        raise
      end

      def announce!
        connection_info = connection_description
        logger.info "Broadcasting PostgreSQL channel: #{channel} (#{connection_info})"
      end

      private

      # Abstract methods to be implemented by subclasses
      def notify_with_payload(payload)
        raise NotImplementedError, "Subclass must implement notify_with_payload"
      end

      def connection_description
        raise NotImplementedError, "Subclass must implement connection_description"
      end

      def execute_with_connection(&block)
        raise NotImplementedError, "Subclass must implement execute_with_connection"
      end

      # Large payload handling (shared logic)
      def large_payload?(payload)
        payload.bytesize > AnyCable.config.postgres_large_payload_threshold
      end

      def store_large_payload(payload)
        execute_with_connection do |conn|
          store_payload_in_table(conn, payload)
        end
      end

      def store_payload_in_table(conn, payload)
        ensure_table_exists(conn)

        payload_id = SecureRandom.uuid
        encrypted_id = encrypt_payload_id(payload_id)

        # Store the large payload in the table
        escaped_table = conn.escape_identifier(table_name)
        escaped_id = conn.escape_literal(payload_id)
        escaped_payload = conn.escape_literal(payload)

        conn.exec("INSERT INTO #{escaped_table} (id, payload, created_at) VALUES (#{escaped_id}, #{escaped_payload}, NOW())")

        # Periodically clean up old payloads
        @cleanup_counter += 1
        cleanup_old_payloads(conn) if @cleanup_counter >= CLEANUP_FREQUENCY

        "#{LARGE_PAYLOAD_MARKER}#{encrypted_id}"
      end

      def notify_connection(conn, payload)
        escaped_channel = conn.escape_identifier(channel)
        escaped_payload = conn.escape_literal(payload)
        conn.exec("NOTIFY #{escaped_channel}, #{escaped_payload}")
      end

      def ensure_table_exists(conn)
        return if @table_created

        escaped_table = conn.escape_identifier(table_name)

        # Create unlogged table for better performance
        conn.exec(<<~SQL)
          CREATE UNLOGGED TABLE IF NOT EXISTS #{escaped_table} (
            id UUID PRIMARY KEY,
            payload TEXT NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL
          )
        SQL

        # Create index for cleanup queries
        conn.exec("CREATE INDEX IF NOT EXISTS #{escaped_table}_created_at_idx ON #{escaped_table} (created_at)")

        @table_created = true
      rescue PG::Error => e
        # Ignore if table already exists or other harmless errors
        logger.debug "Table creation warning: #{e.message}"
        @table_created = true
      end

      def cleanup_old_payloads(conn)
        @cleanup_counter = 0

        escaped_table = conn.escape_identifier(table_name)

        # Delete payloads older than 1 hour
        conn.exec("DELETE FROM #{escaped_table} WHERE created_at < NOW() - INTERVAL '1 hour'")
      rescue PG::Error => e
        logger.debug "Cleanup warning: #{e.message}"
      end

      def table_name
        AnyCable.config.postgres_large_payload_table
      end

      def encrypt_payload_id(payload_id)
        # Simple base64 encoding for now - could be enhanced with proper encryption
        Base64.strict_encode64(payload_id)
      end
    end
  end
end
