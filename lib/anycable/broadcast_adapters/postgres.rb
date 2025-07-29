# frozen_string_literal: true

require_relative "pg/postgres_direct"
require_relative "pg/postgres_active_record"

module AnyCable
  module BroadcastAdapters
    # PostgreSQL adapter for broadcasting using NOTIFY/LISTEN with large payload support.
    #
    # Automatically selects the appropriate implementation:
    # - In Rails environments: Uses ActiveRecord connection pool (PostgresActiveRecord)
    # - Non-Rails environments: Uses direct PostgreSQL connection (PostgresDirect)
    #
    # Example:
    #
    #   AnyCable.broadcast_adapter = :postgres
    #
    # It uses PostgreSQL configuration from global AnyCable config
    # by default, or ActiveRecord's database configuration in Rails.
    #
    # You can override connection params:
    #
    #   AnyCable.broadcast_adapter = :postgres, {
    #     host: "localhost",
    #     port: 5432,
    #     dbname: "mydb",
    #     channel: "_any_cable_"
    #   }
    #
    # Large payloads (> postgres_large_payload_threshold bytes) are automatically
    # stored in an unlogged table and broadcast as markers for efficiency.
    class Postgres < Base
      attr_reader :adapter

      def initialize(channel: AnyCable.config.postgres_channel, **options)
        @adapter = if rails_available? && options.empty?
          PostgresActiveRecord.new(channel: channel)
        else
          PostgresDirect.new(channel: channel, **options)
        end
      end

      # Delegate all methods to the selected adapter
      def raw_broadcast(payload)
        adapter.raw_broadcast(payload)
      end

      def announce!
        adapter.announce!
      end

      def close
        adapter.close
      end

      def channel
        adapter.channel
      end

      # For backward compatibility with tests
      def connection
        adapter.respond_to?(:connection) ? adapter.connection : nil
      end

      def use_rails_connection
        adapter.is_a?(PostgresActiveRecord)
      end

      private

      def rails_available?
        defined?(::Rails) && defined?(::ActiveRecord::Base)
      end
    end
  end
end
