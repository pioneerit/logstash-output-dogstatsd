# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/dogstatsd"
require "logstash/codecs/plain"
require "logstash/event"
require_relative "../spec_helper"

describe LogStash::Outputs::Dogstatsd do
  let(:host) { "127.0.0.1" }
  let(:port) { rand(2000..10000) }

  let(:base) do
    { "host" => host, "port" => port }
  end

  let!(:server) { StatsdServer.new.run(port) }

  after(:each) do
    server.close
  end

  describe "setup" do
    let(:event) { LogStash::Event.new }
    let(:output) { described_class.new(base) }

    describe "registration and close" do
      it "should register without errors" do
        output = LogStash::Plugin.lookup("output", "dogstatsd").new
        expect { output.register }.to_not raise_error
      end
    end

    describe "receive message" do
      before do
        output.register
      end

      subject { output.receive(event) }

      it "returns true" do
        expect(subject).to eq(true)
      end
    end
  end

  describe "#receive" do
    let(:event) { LogStash::Event.new(properties) }
    subject { described_class.new(config) }

    before(:each) do
      subject.register
    end

    [ "increment", "decrement" ].each do |type|
      context "#{type} metrics" do
        let(:name) { "foo" }
        let(:value) { type === "increment" ? 1 : -1 }

        let(:config) do
          base.merge({ type => [ "%{metric_name}" ] })
        end

        let(:properties) do
          { "metric_name" => name }
        end

        it "should receive data send to the server" do
          subject.receive(event)

          try {
            expect(server.received).to include("#{name}:#{value}|c")
          }
        end

        context "#{type} metrics with tags" do
          let(:config_tags) { [ "host:server123" ] }
          let(:event_tags) { [ "env:test" ] }

          let(:config) do
            base.merge({ type => [ "%{metric_name}"] , "metric_tags" => config_tags })
          end

          let(:properties) do
            { "metric_name" => name, "metric_tags" => event_tags  }
          end

          it "should receive data send to the server" do
            subject.receive(event)

            try {
              tag_string = (config_tags + event_tags).join(',')
              expect(server.received).to include("#{name}:#{value}|c|##{tag_string}")
            }
          end
        end

      end
    end

    [ "count", "gauge", "histogram", "set" ].each do |type|
      context "#{type} metrics" do
        let(:name) { "foo.bar" }
        let(:value) { rand(2000..10000) }
        let(:t) { type[0] }

        let(:config) do
          base.merge({ type => { "%{metric_name}" => "%{metric_value}" } })
        end

        let(:properties) do
          { "metric_name" => name, "metric_value" => value }
        end

        it "should receive data send to the server" do
          subject.receive(event)
          try {
            expect(server.received).to include("#{name}:#{value}|#{t}")
          }
        end

      end
    end

  end
end
