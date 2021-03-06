Fan Monitor Demo
================

Spark Streaming Job for devicehive vibrating fan demo

To Configure
--------

You will need to update AppProperties object in FanMonitor.scala

* authToken - authentication token for Devicehive RESTful API
* mac - mac address of LED lamp (or anu other triggering device)
* uuid - it's uuid
* valueOn - notification value to indicate, that vibration is out of balance
* valueOff - notification value to indicate, that stabilisation occurred
* notificationNameToListen - name of notifications that contains vibration value in the parameters
* notificationUuidToListen - their uuid

To Build
--------

You will need sbt tool to be installed.

> sbt assembly

Job jar will be located in target/scala-2.10/fandemo.jar

To Run
------

To run locally with spark, in the spark distro folder:

> bin/spark-submit.cmd --master "local[*]" --class com.devicehive.FanMonitor <job-jar-location>/fanmonitor.jar <zookeeper-host>:2181 5 http://localhost:8080/rest 0.11

Via arguments it gets:
* Zookeeper address (host:port)
* Events window size (in seconds)
* RESTful API URL to receive state changes (oscillation variance is above or below threshold)
* optional threshold (defulat is 0.2)
