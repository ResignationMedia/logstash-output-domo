# encoding: utf-8
require "logstash/outputs/base"

# An domo output that does nothing.
class LogStash::Outputs::Domo < LogStash::Outputs::Base
  config_name "domo"

  public
  def register
  end # def register

  public
  def receive(event)
    return "Event received"
  end # def event
end # class LogStash::Outputs::Domo
