About
====================
zkdemo
Based on zookeeper-book-example

Requirements
====================
Requires Java 8 and Maven to run.

Build
====================
1. check out
2. mvn clean install

Run
====================
1. MasterAsync: monitor zk and assign task to worker
2. WorkerAsync: worker get assigned task and update task state
3. TaskCreatorAsync: create tasks
4. StateReporter: show zk nodes state
