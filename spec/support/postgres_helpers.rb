# frozen_string_literal: true

module PostgresHelpers
  def postgres_available?
    return @postgres_available if defined?(@postgres_available)

    @postgres_available = begin
      return false unless defined?(PG)
      return false unless ENV["DATABASE_URL"]

      conn = PG.connect(ENV["DATABASE_URL"])
      conn.close
      true
    rescue PG::Error, NameError
      false
    end
  end

  def skip_unless_postgres
    skip "Set DATABASE_URL to run PostgreSQL integration tests" unless postgres_available?
  end

  def with_postgres_connection
    skip_unless_postgres

    conn = PG.connect(ENV.fetch("DATABASE_URL"))
    yield conn
  ensure
    conn&.close
  end

  def create_postgres_adapter(channel: nil, **options)
    skip_unless_postgres

    test_channel = channel || "_anycable_test_#{SecureRandom.hex(4)}"

    AnyCable.config.postgres_url = ENV.fetch("DATABASE_URL")
    AnyCable.config.postgres_channel = test_channel

    adapter = AnyCable::BroadcastAdapters::Postgres.new(**options)

    if block_given?
      begin
        yield adapter
      ensure
        begin
          adapter.close
        rescue
          nil
        end
        AnyCable.config.reload
      end
    else
      adapter
    end
  end

  def with_postgres_listener(channel)
    skip_unless_postgres

    listener = PG.connect(ENV.fetch("DATABASE_URL"))
    escaped_channel = listener.escape_identifier(channel)
    listener.exec("LISTEN #{escaped_channel}")

    yield listener
  ensure
    if listener
      begin
        listener.exec("UNLISTEN #{escaped_channel}")
      rescue
        nil
      end
      begin
        listener.close
      rescue
        nil
      end
    end
  end

  # Mock Rails with real PostgreSQL connections for testing
  def with_rails_postgres_integration
    skip_unless_postgres

    # Save original constants
    original_rails = defined?(Rails) ? Rails : nil
    original_ar = defined?(ActiveRecord) ? ActiveRecord : nil
    defined?(ActiveRecord::Base) ? ActiveRecord::Base : nil

    begin
      # Mock Rails environment
      stub_const("Rails", Module.new)

      # Create ActiveRecord::Base that uses real PostgreSQL
      ar_base_class = Class.new do
        def self.connection_pool
          @pool ||= Object.new.tap do |pool|
            def pool.with_connection
              ar_conn = Object.new

              def ar_conn.raw_connection
                PG.connect(ENV.fetch("DATABASE_URL"))
              end

              real_conn = ar_conn.raw_connection
              yield ar_conn
              real_conn.close
            end
          end
        end

        def self.connection_config
          uri = URI.parse(ENV.fetch("DATABASE_URL"))
          {
            adapter: "postgresql",
            host: uri.host,
            port: uri.port,
            database: uri.path&.sub(/^\//, ""),
            username: uri.user,
            password: uri.password
          }.compact
        end
      end

      stub_const("ActiveRecord", Module.new)
      stub_const("ActiveRecord::Base", ar_base_class)

      yield
    ensure
      # Restore original constants if they existed
      if original_rails
        stub_const("Rails", original_rails)
      else
        begin
          Object.send(:remove_const, :Rails) if defined?(Rails)
        rescue NameError
          # Already removed, ignore
        end
      end

      if original_ar
        stub_const("ActiveRecord", original_ar)
      else
        begin
          Object.send(:remove_const, :ActiveRecord) if defined?(ActiveRecord)
        rescue NameError
          # Already removed, ignore
        end
      end
    end
  end
end

if defined?(RSpec)
  RSpec.configure do |config|
    config.include PostgresHelpers, type: :postgres_integration
  end
end
