# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/dead_letter_queue"
require "concurrent"

describe LogStash::Inputs::DeadLetterQueue do
  subject { LogStash::Inputs::DeadLetterQueue.new(config) }

  it "should register" do
    expect {subject.register}.to_not raise_error
  end
end
