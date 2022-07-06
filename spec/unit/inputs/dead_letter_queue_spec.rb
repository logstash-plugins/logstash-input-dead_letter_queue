# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/dead_letter_queue"
require "concurrent"

describe LogStash::Inputs::DeadLetterQueue do
  let(:pipeline_id) { SecureRandom.hex(8)}
  let(:path) { Dir.tmpdir }
  let(:directory) { File.join(path, pipeline_id)}
  let(:config) do
    { "path" => path,
      "pipeline_id" => pipeline_id}
  end

  subject { LogStash::Inputs::DeadLetterQueue.new(config) }

  before(:each) do
    Dir.mkdir(directory)
  end

  it 'should register' do
    expect {subject.register}.to_not raise_error
  end

  after(:each) do
    FileUtils.remove_entry_secure directory
  end

  context "when clean_consumed is enabled" do
    let(:config) { super().merge!("clean_consumed" => true) }

    it "shouldn't register" do
      expect {subject.register}.to_not raise_error
    end

    context "and commit_offsets is not" do
      let(:config) { super().merge!("commit_offsets" => false) }
      it "shouldn't register" do
        expect {subject.register}.to raise_error
      end
    end
  end

  context 'test with real DLQ file' do
    let(:dlq_dir) { Stud::Temporary.directory }
    let(:fixture_dir) { File.expand_path(File.join(File.dirname(__FILE__),"fixtures", "main")) }
    let(:plugin) { LogStash::Inputs::DeadLetterQueue.new({ "path" => dlq_dir, "commit_offsets" => true }) }
    let(:queue) { Queue.new }

    before do
      FileUtils.cp_r fixture_dir, dlq_dir
    end

    it 'reserve original metadata' do
      plugin.register
      t = Thread.new {
        begin
          plugin.run(queue)
        rescue java.nio.file.ClosedWatchServiceException => e
        end
      }

      event = queue.pop
      expect(event.get("[@metadata][some_key]")).to eq("some_value")
      expect(event.get("[@metadata][dead_letter_queue][reason]")).to match(/Could not index event to Elasticsearch. status: 400/)

      plugin.stop
      t.join(1000)
    end
  end
end
