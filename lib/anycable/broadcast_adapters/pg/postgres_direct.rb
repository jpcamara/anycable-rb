# frozen_string_literal: true

require_relative "postgres_base"

module AnyCable
  module BroadcastAdapters
    # Direct PostgreSQL adapter for broadcasting using NOTIFY/LISTEN.
    #
    # Uses a direct PG connection for broadcasting. Ideal for non-Rails
    # applications or when you need full control over the connection.
    #
    # Example:
    #
    #   AnyCable.broadcast_adapter = :postgres_direct, {
    #     host: "localhost",
    #     port: 5432,
    #     dbname: "mydb",
    #     channel: "_any_cable_"
    #   }
    class PostgresDirect < PostgresBase
      attr_reader :connection

      def initialize(channel: AnyCable.config.postgres_channel, **options)
        super(channel: channel)

        options = AnyCable.config.to_postgres_params.merge(options)
        @connection = PG.connect(**options)
      end

      def close
        connection&.close
      end

      private

      def notify_with_payload(payload)
        notify_connection(connection, payload)
      end

      def connection_description
        "direct connection"
      end

      def execute_with_connection
        yield(connection)
      end
    end
  end
end
