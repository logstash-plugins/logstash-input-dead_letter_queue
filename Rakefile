require "logstash/devutils/rake"

task :vendor => "gradle.properties" do
  sh "#{File.join(Dir.pwd, 'gradlew')} clean vendor"
end

file "gradle.properties" do
  root_dir = File.dirname(__FILE__)
  gradle_properties_file = "#{root_dir}/gradle.properties"
  # find the path to the logstash-core gem
  lsc_path = Bundler.rubygems.find_name("logstash-core").first.full_gem_path
  FileUtils.rm_f(gradle_properties_file)
  File.open(gradle_properties_file, "w") do |f|
    f.puts "logstashCoreGemPath=#{lsc_path}"
  end
  puts "-------------------> Wrote #{gradle_properties_file}"
  puts File.read(gradle_properties_file)
end
