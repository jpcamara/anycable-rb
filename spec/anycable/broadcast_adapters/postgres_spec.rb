# frozen_string_literal: true

require "spec_helper"

require "anycable/broadcast_adapters/postgres"

describe AnyCable::BroadcastAdapters::Postgres do
  let(:pg_conn) { double("pg_conn") }

  before do
    config.postgres_url = "postgres://user:pass@postgres-1:5433/test_db"
    config.postgres_channel = "_test_"
    config.postgres_large_payload_threshold = 8000
    config.postgres_large_payload_table = "anycable_large_payloads"

    allow(PG).to receive(:connect) { pg_conn }
    allow(pg_conn).to receive(:escape_identifier) { |name| "\"#{name}\"" }
    allow(pg_conn).to receive(:escape_literal) { |payload| "'#{payload.gsub("'", "''")}'" }
    allow(pg_conn).to receive(:exec)
  end

  after { AnyCable.config.reload }

  let(:config) { AnyCable.config }

  it "uses config options by default" do
    adapter = described_class.new
    expect(adapter.channel).to eq "_test_"
    expect(PG).to have_received(:connect)
      .with(host: "postgres-1", port: 5433, dbname: "test_db", user: "user", password: "pass")
  end

  it "uses override config params" do
    adapter = described_class.new(host: "postgres.example.com", port: 5432, channel: "_zyx_")
    expect(adapter.channel).to eq "_zyx_"
    expect(PG).to have_received(:connect)
      .with(host: "postgres.example.com", port: 5432, dbname: "test_db", user: "user", password: "pass")
  end

  describe "#announce" do
    around do |ex|
      old_logger = AnyCable.logger
      AnyCable.remove_instance_variable(:@logger)
      ex.run
      AnyCable.logger = old_logger
      AnyCable.config.reload
    end

    specify do
      expect { described_class.new.announce! }.to output(/Broadcasting PostgreSQL channel: _test_/).to_stdout_from_any_process
    end
  end

  describe "#broadcast" do
    it "sends NOTIFY with stream data to channel for small payloads" do
      adapter = described_class.new
      adapter.broadcast("notification", "hello!")

      expected_payload = {stream: "notification", data: "hello!"}.to_json
      expect(pg_conn).to have_received(:exec).with(
        "NOTIFY \"_test_\", '#{expected_payload.gsub("'", "''")}'"
      )
    end

    it "stores large payloads in table and sends marker" do
      large_data = "x" * 9000  # Exceeds threshold
      allow(SecureRandom).to receive(:uuid).and_return("test-uuid-123")
      allow(Base64).to receive(:strict_encode64).with("test-uuid-123").and_return("encoded-id")

      adapter = described_class.new
      adapter.broadcast("large_stream", large_data)

      expected_payload = {stream: "large_stream", data: large_data}.to_json

      # Should create table
      expect(pg_conn).to have_received(:exec).with(/CREATE UNLOGGED TABLE IF NOT EXISTS/)

      # Should insert payload
      expect(pg_conn).to have_received(:exec).with(/INSERT INTO.*test-uuid-123.*#{Regexp.escape(expected_payload)}/)

      # Should notify with marker
      marker_payload = "__large_payload:encoded-id"
      expect(pg_conn).to have_received(:exec).with(
        "NOTIFY \"_test_\", '#{marker_payload}'"
      )
    end
  end

  describe "#broadcast_command" do
    it "sends NOTIFY with command data to channel for small commands" do
      adapter = described_class.new
      adapter.broadcast_command("disconnect", identifier: "42")

      expected_payload = {command: "disconnect", payload: {identifier: "42"}}.to_json
      expect(pg_conn).to have_received(:exec).with(
        "NOTIFY \"_test_\", '#{expected_payload.gsub("'", "''")}'"
      )
    end
  end

  describe "large payload configuration" do
    it "uses configurable threshold" do
      config.postgres_large_payload_threshold = 50  # Very small threshold for testing

      adapter = described_class.new
      adapter.broadcast("test", "short message")  # Under threshold

      expected_payload = {stream: "test", data: "short message"}.to_json
      expect(pg_conn).to have_received(:exec).with(
        "NOTIFY \"_test_\", '#{expected_payload.gsub("'", "''")}'"
      )
    end

    it "handles large payloads when threshold is exceeded" do
      config.postgres_large_payload_threshold = 10  # Very small threshold
      large_message = "this message is longer than ten characters"

      allow(SecureRandom).to receive(:uuid).and_return("test-uuid-456")
      allow(Base64).to receive(:strict_encode64).with("test-uuid-456").and_return("encoded-456")

      adapter = described_class.new
      adapter.broadcast("test", large_message)

      expect(pg_conn).to have_received(:exec).with(/CREATE UNLOGGED TABLE/)
      expect(pg_conn).to have_received(:exec).with(/INSERT INTO/)
      expect(pg_conn).to have_received(:exec).with(
        "NOTIFY \"_test_\", '__large_payload:encoded-456'"
      )
    end
  end

  describe "#close" do
    it "closes the PostgreSQL connection" do
      allow(pg_conn).to receive(:close)

      adapter = described_class.new
      adapter.close

      expect(pg_conn).to have_received(:close)
    end
  end

  describe "error handling" do
    it "logs and re-raises PG::Error" do
      allow(pg_conn).to receive(:exec).and_raise(PG::Error.new("Connection lost"))
      allow(AnyCable.logger).to receive(:error)

      adapter = described_class.new

      expect { adapter.broadcast("test", "data") }.to raise_error(PG::Error, "Connection lost")
      expect(AnyCable.logger).to have_received(:error).with("PostgreSQL broadcast failed: Connection lost")
    end
  end

  describe "Rails integration" do
    let(:ar_connection) { double("ar_connection") }
    let(:ar_raw_connection) { double("ar_raw_connection") }
    let(:ar_connection_pool) { double("ar_connection_pool") }

    before do
      # Create a proper mock class for ActiveRecord::Base
      ar_base_class = Class.new do
        def self.connection_pool
          @mock_pool
        end

        def self.connection_pool=(pool)
          @mock_pool = pool
        end
      end

      # Mock Rails and ActiveRecord
      stub_const("Rails", Module.new)
      stub_const("ActiveRecord", Module.new)
      stub_const("ActiveRecord::Base", ar_base_class)

      # Set up the mock connection pool
      ar_base_class.connection_pool = ar_connection_pool
      allow(ar_connection_pool).to receive(:with_connection).and_yield(ar_connection)
      allow(ar_connection).to receive(:raw_connection).and_return(ar_raw_connection)
      allow(ar_raw_connection).to receive(:escape_identifier) { |name| "\"#{name}\"" }
      allow(ar_raw_connection).to receive(:escape_literal) { |payload| "'#{payload.gsub("'", "''")}'" }
      allow(ar_raw_connection).to receive(:exec)
    end

    it "uses Rails connection pool when Rails is available" do
      adapter = described_class.new
      expect(adapter.use_rails_connection).to be true
      expect(adapter.connection).to be_nil
    end

    it "broadcasts using Rails connection pool" do
      adapter = described_class.new
      adapter.broadcast("rails_stream", "Rails message!")

      expected_payload = {stream: "rails_stream", data: "Rails message!"}.to_json
      expect(ar_connection_pool).to have_received(:with_connection)
      expect(ar_raw_connection).to have_received(:exec).with(
        "NOTIFY \"_test_\", '#{expected_payload.gsub("'", "''")}'"
      )
    end

    it "announces Rails connection usage" do
      old_logger = AnyCable.logger
      begin
        AnyCable.remove_instance_variable(:@logger)
      rescue
        nil
      end

      expect { described_class.new.announce! }.to output(/via Rails\/ActiveRecord/).to_stdout_from_any_process

      AnyCable.logger = old_logger
      AnyCable.config.reload
    end

    it "doesn't close Rails connections" do
      adapter = described_class.new
      expect(adapter.connection).to be_nil

      # Should not raise any errors
      adapter.close
    end

    it "falls back to direct connection when custom options provided" do
      adapter = described_class.new(host: "custom-host")
      expect(adapter.use_rails_connection).to be false
      expect(PG).to have_received(:connect)
    end
  end
end
