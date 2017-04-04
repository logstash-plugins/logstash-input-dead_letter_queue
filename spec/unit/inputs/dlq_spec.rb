# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/dlq"
require "concurrent"

describe LogStash::Inputs::Dlq do
  subject { LogStash::Inputs::Dlq.new(config) }

  it "should register" do
    expect {subject.register}.to_not raise_error
  end
end
