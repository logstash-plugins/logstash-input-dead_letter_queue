bundle install
bundle exec rake vendor
./gradlew test -i
bundle exec rspec spec
# bundle exec rake test:integration:setup TODO
# bundle exec rspec spec --tag integration TODO
