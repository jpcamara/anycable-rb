# frozen_string_literal: true

require_relative "postgres_base"

module AnyCable
  module BroadcastAdapters
    # ActiveRecord-integrated PostgreSQL adapter for broadcasting using NOTIFY/LISTEN.
    #
    # Uses Rails' ActiveRecord connection pool for better integration and
    # connection management in Rails applications.
    #
    # Example:
    #
    #   AnyCable.broadcast_adapter = :postgres_active_record
    #
    # This adapter automatically uses the Rails database configuration
    # and connection pool, ensuring optimal resource usage.
    class PostgresActiveRecord < PostgresBase
      def initialize(channel: AnyCable.config.postgres_channel)
        super

        unless rails_available?
          raise "PostgresActiveRecord adapter requires Rails and ActiveRecord"
        end
      end

      def close
        # Rails manages the connection pool, nothing to close
      end

      private

      def notify_with_payload(payload)
        with_rails_connection do |conn|
          notify_connection(conn, payload)
        end
      end

      def connection_description
        "via Rails/ActiveRecord"
      end

      def execute_with_connection
        with_rails_connection do |conn|
          yield(conn)
        end
      end

      def rails_available?
        defined?(::Rails) && defined?(::ActiveRecord::Base)
      end

      def with_rails_connection
        ::ActiveRecord::Base.connection_pool.with_connection do |ar_conn|
          # Get the raw PG connection from ActiveRecord
          raw_conn = ar_conn.raw_connection
          yield(raw_conn)
        end
      end
    end
  end
end
