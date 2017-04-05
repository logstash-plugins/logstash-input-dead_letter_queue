require 'logstash/namespace'
require 'logstash/inputs/base'
require 'logstash-input-dead_letter_queue.jar'

# Logstash input to read events from a dead letter queue
class LogStash::Inputs::DeadLetterQueue < LogStash::Inputs::Base
  config_name 'dead_letter_queue'

  default :codec, 'plain'

  config :path, :validate => :path, :required => true
  config :sincedb_path, :validate => :string, :required => false
  config :commit_offsets, :validate => :boolean, :default => true
  config :start_timestamp, :validate => :string, :required => false

  public
  def register
    if @sincedb_path.nil?
      datapath = File.join(LogStash::SETTINGS.get_value("path.data"), "plugins", "inputs", "dead_letter_queue")
      # Ensure that the filepath exists before writing, since it's deeply nested.
      FileUtils::mkdir_p datapath
      @sincedb_path = File.join(datapath, ".sincedb_" + Digest::MD5.hexdigest(@path))
      puts @sincedb_path
    elsif File.directory?(@sincedb_path)
        raise ArgumentError.new("The \"sincedb_path\" argument must point to a file, received a directory: \"#{@sincedb_path}\"")
    end

    path = java.nio.file.Paths.get(@path)
    sincedb_path = @sincedb_path ? java.nio.file.Paths.get(@sincedb_path) : nil
    start_timestamp = @start_timestamp ? org.logstash.Timestamp.new(@start_timestamp) : nil
    @inner_plugin = org.logstash.input.DeadLetterQueueInputPlugin.new(path, @commit_offsets, sincedb_path, start_timestamp)
    @inner_plugin.register
  end # def register

  public
  def run(logstash_queue)
    @inner_plugin.run do |entry|
      event = LogStash::Event.new(entry.event.toMap())
      event.set("[@metadata][dead_letter_queue][plugin_type]", entry.plugin_type)
      event.set("[@metadata][dead_letter_queue][plugin_id]", entry.plugin_id)
      event.set("[@metadata][dead_letter_queue][reason]", entry.reason)
      event.set("[@metadata][dead_letter_queue][entry_time]", entry.entry_time)
      logstash_queue << event
    end
  end # def run

  public
  def stop
    @inner_plugin.close
  end
end
