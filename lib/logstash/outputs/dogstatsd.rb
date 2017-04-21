# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "datadog/statsd"
# Example:
# [source,ruby]
# output {
#   dogstatsd {
#     metric_tags => ["host:%{host}","role:foo"]
#     count => {
#       "http.bytes" => "%{bytes}"
#     }
#   }
# }

class LogStash::Outputs::Dogstatsd < LogStash::Outputs::Base
  ## Regex stolen from statsd code
  RESERVED_CHARACTERS_REGEX = /[\:\|\@]/

  config_name "dogstatsd"

  # The hostname or IP address of the dogstatsd server.
  config :host, :validate => :string, :default => "localhost"

  # The port to connect to on your dogstatsd server.
  config :port, :validate => :number, :default => 8125

  # An increment metric. Metric names as array. `%{fieldname}` substitutions are
  # allowed in the metric names.
  config :increment, :validate => :array, :default => []

  # A decrement metric. Metric names as array. `%{fieldname}` substitutions are
  # allowed in the metric names.
  config :decrement, :validate => :array, :default => []

  # A histogram metric, which a statsd timing but conceptually maps to any
  # numeric value, not just durations. `metric_name => value` as hash. `%{fieldname}`
  # substitutions are allowed in the metric names.
  config :histogram, :validate => :hash, :default => {}

  # A count metric. `metric_name => count` as hash. `%{fieldname}` substitutions are
  # allowed in the metric names.
  config :count, :validate => :hash, :default => {}

  # A set metric. `metric_name => "string"` to append as hash. `%{fieldname}`
  # substitutions are allowed in the metric names.
  config :set, :validate => :hash, :default => {}

  # A gauge metric. `metric_name => gauge` as hash. `%{fieldname}` substitutions are
  # allowed in the metric names.
  config :gauge, :validate => :hash, :default => {}

  # The sample rate for the metric.
  config :sample_rate, :validate => :number, :default => 1

  # The tags to apply to each metric.
  config :metric_tags, :validate => :array, :default => []

  public
  def register
    @client = Datadog::Statsd.new(@host, @port)
  end

  public
  def receive(event)
    @logger.debug? and @logger.debug("Event: #{event}")

    tags = with_default(@metric_tags, []) + with_default(event.get("metric_tags"), [])
    sample_rate = with_default(event.get("sample_rate"), @sample_rate)

    metric_opts = {
      :sample_rate=> sample_rate,
      :tags => tags.collect { |t| event.sprintf(t) }
    }

    @increment.each do |metric|
      @client.increment(event.sprintf(metric), metric_opts)
    end

    @decrement.each do |metric|
      @client.decrement(event.sprintf(metric), metric_opts)
    end

    @count.each do |metric, val|
      @client.count(event.sprintf(metric), event.sprintf(val), metric_opts)
    end

    @histogram.each do |metric, val|
      @client.histogram(event.sprintf(metric), event.sprintf(val), metric_opts)
    end

    @set.each do |metric, val|
      @client.set(event.sprintf(metric), event.sprintf(val), metric_opts)
    end

    @gauge.each do |metric, val|
      @client.gauge(event.sprintf(metric), event.sprintf(val), metric_opts)
    end

    return true
  end

  public
  def close
    @client.close
  end

  private
  def with_default(target ,default)
    (target || default)
  end
end
