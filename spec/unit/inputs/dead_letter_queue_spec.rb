# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/dead_letter_queue"
require "concurrent"

describe LogStash::Inputs::DeadLetterQueue do
  let(:pipeline_id) { SecureRandom.hex(8)}
  let(:path) { Dir.tmpdir }
  let(:directory) { File.join(path, pipeline_id)}

  subject { LogStash::Inputs::DeadLetterQueue.new({ "path" => path,
                                                    "pipeline_id" => pipeline_id}) }

  before(:each) do
    Dir.mkdir(directory)
  end

  it 'should register' do
    expect {subject.register}.to_not raise_error
  end


  after(:each) do
    FileUtils.remove_entry_secure directory
  end
end
