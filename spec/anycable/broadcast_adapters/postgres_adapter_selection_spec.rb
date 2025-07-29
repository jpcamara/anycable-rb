# frozen_string_literal: true

require "spec_helper"

require "anycable/broadcast_adapters/postgres"

describe "PostgreSQL Adapter Selection" do
  let(:config) { AnyCable.config }
  let(:pg_conn) { double("pg_conn") }

  before do
    config.postgres_url = "postgres://user:pass@postgres-1:5433/test_db"
    config.postgres_channel = "_test_"

    allow(PG).to receive(:connect) { pg_conn }
    allow(pg_conn).to receive(:escape_identifier) { |name| "\"#{name}\"" }
    allow(pg_conn).to receive(:escape_literal) { |payload| "'#{payload.gsub("'", "''")}'" }
    allow(pg_conn).to receive(:exec)
  end

  after { AnyCable.config.reload }

  describe "adapter selection logic" do
    it "selects PostgresDirect when Rails is not available" do
      adapter = AnyCable::BroadcastAdapters::Postgres.new

      expect(adapter.adapter).to be_a(AnyCable::BroadcastAdapters::PostgresDirect)
      expect(adapter.use_rails_connection).to be_falsy
      expect(adapter.connection).to eq(pg_conn)
    end

    it "selects PostgresDirect when custom options are provided" do
      # Even if Rails were available, custom options should force direct connection
      adapter = AnyCable::BroadcastAdapters::Postgres.new(host: "custom-host")

      expect(adapter.adapter).to be_a(AnyCable::BroadcastAdapters::PostgresDirect)
      expect(adapter.use_rails_connection).to be_falsy
    end

    it "selects PostgresActiveRecord when Rails is available and no custom options" do
      # Mock Rails and ActiveRecord
      ar_base_class = Class.new do
        def self.connection_pool
          @mock_pool ||= Object.new.tap do |pool|
            def pool.with_connection
              ar_conn = Object.new
              def ar_conn.raw_connection
                @raw_conn ||= double("raw_pg_conn")
              end
              yield ar_conn
            end
          end
        end
      end

      stub_const("Rails", Module.new)
      stub_const("ActiveRecord", Module.new)
      stub_const("ActiveRecord::Base", ar_base_class)

      adapter = AnyCable::BroadcastAdapters::Postgres.new

      expect(adapter.adapter).to be_a(AnyCable::BroadcastAdapters::PostgresActiveRecord)
      expect(adapter.use_rails_connection).to be_truthy
      expect(adapter.connection).to be_nil  # No direct connection in Rails mode
    end
  end

  describe "delegation behavior" do
    let(:direct_adapter) { AnyCable::BroadcastAdapters::PostgresDirect.new }
    let(:postgres_adapter) { AnyCable::BroadcastAdapters::Postgres.new }

    before do
      allow(AnyCable::BroadcastAdapters::PostgresDirect).to receive(:new).and_return(direct_adapter)
    end

    it "delegates broadcast methods to underlying adapter" do
      allow(direct_adapter).to receive(:raw_broadcast)

      postgres_adapter.raw_broadcast("test payload")

      expect(direct_adapter).to have_received(:raw_broadcast).with("test payload")
    end

    it "delegates announce to underlying adapter" do
      allow(direct_adapter).to receive(:announce!)

      postgres_adapter.announce!

      expect(direct_adapter).to have_received(:announce!)
    end

    it "delegates close to underlying adapter" do
      allow(direct_adapter).to receive(:close)

      postgres_adapter.close

      expect(direct_adapter).to have_received(:close)
    end

    it "delegates channel to underlying adapter" do
      allow(direct_adapter).to receive(:channel).and_return("_test_channel_")

      expect(postgres_adapter.channel).to eq("_test_channel_")
    end
  end
end
