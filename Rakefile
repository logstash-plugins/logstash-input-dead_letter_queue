require "logstash/devutils/rake"

task :default do
  sh('rake -T')
end

task :vendor => "gradle.properties" do
  sh "#{File.join(Dir.pwd, 'gradlew')} vendor"
end

file "gradle.properties" do
  root_dir = File.dirname(__FILE__)
  gradle_properties_file = "#{root_dir}/gradle.properties"
  lsc_path = `bundle show logstash-core`
  FileUtils.rm_f(gradle_properties_file)
  File.open(gradle_properties_file, "w") do |f|
    f.puts "logstashCoreGemPath=#{lsc_path}"
  end
  puts "-------------------> Wrote #{gradle_properties_file}"
  puts File.read(gradle_properties_file)
end
