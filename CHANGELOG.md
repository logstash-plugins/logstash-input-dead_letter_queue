## 2.0.0
  - Introduce the boolean `clean_consumed` setting to enable the automatic removal of completely consumed segments. Requires Logstash 8.4.0 or above [#43](https://github.com/logstash-plugins/logstash-input-dead_letter_queue/pull/43)
  - Exposes metrics about segments and events cleaned by this plugin [#45](https://github.com/logstash-plugins/logstash-input-dead_letter_queue/pull/45)

## 1.1.12
  - Fix: Replace use of block with lambda to fix wrong number of arguments error on jruby-9.3.4.0 [#42](https://github.com/logstash-plugins/logstash-input-dead_letter_queue/pull/42)
  - Refactor: separated sinceDb management is its separate class [#40](https://github.com/logstash-plugins/logstash-input-dead_letter_queue/pull/40)
  - Build: cleanup/review (unused) dependencies [#36](https://github.com/logstash-plugins/logstash-input-dead_letter_queue/pull/36)
  - Build: refactor tasks (runnable on windows) [#37](https://github.com/logstash-plugins/logstash-input-dead_letter_queue/pull/37)

## 1.1.11
  - Fix: pre-flight checks before creating DLQ reader [#35](https://github.com/logstash-plugins/logstash-input-dead_letter_queue/pull/35)

## 1.1.10
  - Fix, avoid Logstash crash on shutdown if DLQ files weren't created [#33](https://github.com/logstash-plugins/logstash-input-dead_letter_queue/pull/33)

## 1.1.9
  - Fix `@metadata` get overwritten by reestablishing metadata that stored in DLQ [#34](https://github.com/logstash-plugins/logstash-input-dead_letter_queue/pull/34)

## 1.1.8
  - Update dependencies for log4j to 2.17.1

## 1.1.7
  - Further update dependencies for log4j (2.17.0) and jackson

## 1.1.6
  - Update dependencies for log4j and jackson [#30](https://github.com/logstash-plugins/logstash-input-dead_letter_queue/pull/30)

## 1.1.5
  - Fix asciidoc formatting in documentation [#21](https://github.com/logstash-plugins/logstash-input-dead_letter_queue/pull/21)

## 1.1.4
  - Fix broken 1.1.3 release

## 1.1.3
  - Docs: Set the default_codec doc attribute.

## 1.1.2
  - Update gemspec summary

## 1.1.1
 - Docs: Add link to conceptual docs about the dead letter queue
 
## 1.1.0
 - Added support for 'add-field' and 'tags' 
 
## 1.0.6
  - Fix some documentation issues

## 1.0.5
 - Internal: Fixed Continuous Integration errors

## 1.0.4
 - Interal: Bump patch level for doc generation

## 1.0.3
 - Docs: Fixed error in example plus made a few edits
 
## 1.0.2
 - internal: renamed DeadLetterQueueWriteManager to DeadLetterQueueWriter in tests
 
## 1.0.1
  - internal: rename DeadLetterQueueManager to DeadLetterQueueReader

## 1.0.0
  - init
