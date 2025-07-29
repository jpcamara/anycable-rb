# frozen_string_literal: true

require "spec_helper"

begin
  require "pg"
  require "anycable/broadcast_adapters/postgres"
  require "support/postgres_helpers"
rescue LoadError => e
  if e.message.include?("pg gem")
    # Skip all tests if pg gem is not available
    RSpec.describe "PostgreSQL Integration (Skipped)" do
      it "requires pg gem" do
        skip "Please install the pg gem to run PostgreSQL integration tests"
      end
    end
  else
    raise
  end
end

# Integration tests with real PostgreSQL
# Set DATABASE_URL to run these tests, e.g.:
# DATABASE_URL=postgres://localhost:5432/anycable_test bundle exec rspec spec/anycable/broadcast_adapters/postgres_integration_spec.rb

if defined?(PG)
  RSpec.describe "PostgreSQL Integration", type: :postgres_integration do
    describe "with real PostgreSQL connection" do
      it "creates adapter and connects successfully" do
        create_postgres_adapter do |adapter|
          expect(adapter).to be_a(AnyCable::BroadcastAdapters::Postgres)
          expect(adapter.connection).to be_a(PG::Connection)
        end
      end

      it "broadcasts messages to PostgreSQL" do
        create_postgres_adapter do |adapter|
          # This should not raise any errors
          expect {
            adapter.broadcast("test_stream", "Hello PostgreSQL!")
            adapter.broadcast_command("disconnect", user_id: 123)
          }.not_to raise_error
        end
      end

      it "handles PostgreSQL NOTIFY correctly" do
        skip_unless_postgres

        test_channel = "_notify_test_#{SecureRandom.hex(4)}"

        create_postgres_adapter(channel: test_channel) do |sender|
          with_postgres_listener(test_channel) do |listener_conn|
            # Send a notification
            test_message = {stream: "integration_test", data: "Real PostgreSQL test!"}.to_json
            sender.raw_broadcast(test_message)

            # Check for notification (with timeout)
            listener_conn.wait_for_notify(1) do |channel, pid, payload|
              expect(channel).to eq(test_channel)
              expect(payload).to eq(test_message)
              expect(pid).to be_a(Integer)
            end
          end
        end
      end

      it "handles connection errors gracefully" do
        skip_unless_postgres

        # Test with invalid connection parameters
        expect {
          AnyCable::BroadcastAdapters::Postgres.new(host: "nonexistent-host")
        }.to raise_error(PG::ConnectionBad)
      end

      it "works with custom channel names" do
        custom_channel = "_custom_test_#{SecureRandom.hex(4)}"

        create_postgres_adapter(channel: custom_channel) do |adapter|
          expect(adapter.channel).to eq(custom_channel)

          # Should not raise errors
          expect {
            adapter.broadcast("custom_test", "Custom channel test")
          }.not_to raise_error
        end
      end

      it "handles large payloads with real PostgreSQL" do
        skip_unless_postgres

        # Set a small threshold for testing
        old_threshold = AnyCable.config.postgres_large_payload_threshold
        AnyCable.config.postgres_large_payload_threshold = 100

        large_payload = "x" * 200  # Exceeds threshold
        test_channel = "_large_test_#{SecureRandom.hex(4)}"

        begin
          create_postgres_adapter(channel: test_channel) do |sender|
            with_postgres_listener(test_channel) do |listener_conn|
              # Send a large payload
              sender.broadcast("large_stream", large_payload)

              # Check for notification (should be a marker)
              listener_conn.wait_for_notify(2) do |channel, pid, payload|
                expect(channel).to eq(test_channel)
                expect(payload).to start_with("__large_payload:")
                expect(pid).to be_a(Integer)

                # Verify the table was created and contains our payload
                table_name = AnyCable.config.postgres_large_payload_table
                escaped_table = listener_conn.escape_identifier(table_name)
                result = listener_conn.exec("SELECT COUNT(*) FROM #{escaped_table}")
                expect(result[0]["count"].to_i).to be > 0
              end
            end
          end
        ensure
          AnyCable.config.postgres_large_payload_threshold = old_threshold
        end
      end
    end

    describe "Rails integration with real PostgreSQL" do
      it "detects Rails absence correctly" do
        skip_unless_postgres

        # Without Rails defined, should use direct connection
        adapter = AnyCable::BroadcastAdapters::Postgres.new
        expect(adapter.use_rails_connection).to be_falsy
        expect(adapter.connection).to be_a(PG::Connection)

        # Should broadcast without errors using direct connection
        expect {
          adapter.broadcast("direct_test", "Direct PostgreSQL connection!")
        }.not_to raise_error

        adapter.close
      end
    end

    describe "URL parsing with real connections" do
      it "parses complex PostgreSQL URLs correctly" do
        skip_unless_postgres

        # Test with a complex URL that includes SSL and other parameters
        base_url = ENV.fetch("DATABASE_URL")
        complex_url = "#{base_url}?sslmode=prefer&connect_timeout=10&application_name=anycable_test"

        config = AnyCable.config
        config.postgres_url = complex_url
        params = config.to_postgres_params

        begin
          # Should parse all parameters correctly
          expect(params).to include(:sslmode, :connect_timeout, :application_name)
          expect(params[:application_name]).to eq("anycable_test")

          # Should still be able to connect
          adapter = AnyCable::BroadcastAdapters::Postgres.new
          expect(adapter.connection).to be_a(PG::Connection)
          adapter.close
        ensure
          AnyCable.config.reload
        end
      end
    end
  end
end
