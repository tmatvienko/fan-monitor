package com.devicehive

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{ SparkConf, HashPartitioner }
import scala.util.Try

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

import dispatch._, Defaults._

case class Notification(id: Int, notification: String, deviceGuid: String,
                        timestamp: String, parameters: String, value: String, deviceId: String)
case class NotificationParams(mac: String, uuid: String, value: String)

case class FanState(initialized: Boolean, vibration: Boolean)

object AppProperties {
  val authToken = "Bearer 1jwKgLYi/CdfBTI9KByfYxwyQ6HUIEfnGSgakdpFjgk="

  val mac = "d05fb831379f"
  val uuid = "fff3"
  val valueOn = "0f0d0300ffffffc800c800c8000059ffff"
  val valueOff = "0f0d0300ffffff0000c800c8000091ffff"

  val notificationNameToListen = "NotificationReceived"
  val notificationUuidToListen = "f000aa1104514000b000000000000000"
}

object FanMonitor extends App {

    implicit val formats = DefaultFormats

    if (args.length < 3) {
      System.err.println(s"""
        |Usage: FanMonitor <zookeeper> <seconds> <url> <threshold> <characteristic>
        |  <zookeeper> zookeeper quorum (list of one or more Kafka zookeper instances
        |  <seconds> monitoring "window" length in seconds
        |  <url> service URL to track monitor state changes; monitor will send POST to this address with state parameter set to 1 (vibration) or 0 (no vibration)
        |  <threshold> vibration threshold (oscilation variance), defaults to 0.2
        |
        """.stripMargin)
      System.exit(1)
    }

    val zkQuorum = args(0)
    val windowSize = args(1).toInt

    val dhRest = args(2)
    val threshold = Try(args(3).toDouble).getOrElse(0.2D)


    val sc = new SparkConf().setAppName("FanMonitor")
    val ssc =  new StreamingContext(sc, Seconds(windowSize))

    ssc.checkpoint("checkpoint")

    val messages = KafkaUtils.createStream(ssc, zkQuorum, "my-consumer-group",
      Map("device_notification" -> 1))

    /* Message sample:
      {"id":372982199,"notification":"NotificationReceived",
       "deviceGuid":"e50d6085-2aba-48e9-b1c3-73c673e414be",
       "timestamp":"2015-05-21T15:10:15.050884",
       "parameters":
           {"jsonString":"{\"mac\":\"d05fb831379f\",\"uuid\":\"fff3\",\"value\":\"0.00005292811793254699\"}"}}
     */

    /*
     * DH requests
     * init:
     * curl "http://52.0.200.198:8080/dh/rest/device/56a17de5-3dfd-4115-970a-f5570b30d85f/command" -H "Authorization: Bearer 1jwKgLYi/CdfBTI9KByfYxwyQ6HUIEfnGSgakdpFjgk=" -H "Content-Type: application/json"  --data-binary "{""timestamp"":1431098561470,""parameters"":{""type"":""DELIGHT"",""mac"":""""},""command"":""init""}"
     * curl "http://52.0.200.198:8080/dh/rest/device/56a17de5-3dfd-4115-970a-f5570b30d85f/command" -H "Authorization: Bearer 1jwKgLYi/CdfBTI9KByfYxwyQ6HUIEfnGSgakdpFjgk=" -H "Content-Type: application/json"  --data-binary "{""timestamp"":1431098561470,""parameters"":{""type"":""SATECHILED-0""",""mac"":""""},""command"":""init""}"
     *
     * update led:
     * curl "http://52.0.200.198:8080/dh/rest/device/56a17de5-3dfd-4115-970a-f5570b30d85f/command" -H "Authorization: Bearer 1jwKgLYi/CdfBTI9KByfYxwyQ6HUIEfnGSgakdpFjgk=" -H "Content-Type: application/json"  --data-binary "{""timestamp"":1431098595709,""parameters"":{""mac"":""f4044c0c58a3"",""uuid"":""fff3"",""value"":""0f0d0300ff00006400000000000067ffff""},""command"":""gatt/write""}"
     * lamp values:
     * ON 0f0d0300ffffffc800c800c8000059ffff
     * OFF 0f0d0300ffffff0000c800c8000091ffff
     */

     def dhCommand(deviceID: String, command: String, params: JsonAST.JObject) = {
       val cmdJson = ("timestamp" -> new java.util.Date().getTime) ~ ("command" -> command) ~ ("parameters" -> params)

       val cmdUrl = url(s"$dhRest/device/$deviceID/command").POST
         .setContentType("application/json", "UTF-8")
         .setHeader("Authorization", AppProperties.authToken)
         .setBody(compact(render(cmdJson)))

       Http(cmdUrl OK as.String)
     }

     def lampValue(vibration: Boolean) =
       if (vibration) AppProperties.valueOn else AppProperties.valueOff

  val updateFanStateIter = (iter: Iterator[(String, Seq[Double], Option[FanState])]) =>
      iter.map {
        case (deviceID, vals, oldState) =>
          val state: FanState = oldState.getOrElse(FanState(false, false))

          val initialized = if (!state.initialized) {
            println("!!! device initialized !!!")
            println(s"""DH: $dhRest/device/$deviceID/command, { "timestamp": 1431098595709, "parameters":{"type":"DELIGHT", "mac":"$AppProperties.mac"},"command":"init" }""")
            dhCommand(deviceID, "init", ("mac" -> AppProperties.mac) ~ ("type" -> "DELIGHT"))

            true
          } else state.initialized

          val vibration = vals.exists(_ > threshold)

          if (state.vibration != vibration) {
            println("!!! vibration state changed !!!")
            println(s"""DH: $dhRest/device/$deviceID/command, { "timestamp": 1431098595709, "parameters":{"mac": "$AppProperties.mac", "uuid":"$AppProperties.uuid", "value": "${lampValue(vibration)}"}, "command": "gatt/write" }""")
            dhCommand(deviceID, "gatt/write", ("mac" -> AppProperties.mac) ~ ("uuid" -> AppProperties.uuid) ~ ("value" -> lampValue(vibration)))
          }
          deviceID -> FanState(initialized, vibration)
      }

    val measurements = messages
      .filter {
        msg =>
          val jsonMsg = parse(msg._2)
          (jsonMsg \\ "notification").extract[String] == AppProperties.notificationNameToListen
      }
      .map {
        msg =>
          val jsonMsg = parse(msg._2)

          val deviceID= (jsonMsg \\ "deviceGuid").extract[String]
          val notification = ((jsonMsg \\ "parameters") \\ "jsonString").extract[String]
          val jsonParams = parse(notification)

          deviceID -> jsonParams.extract[NotificationParams]
      }
      .filter {
        case (deviceID, p) =>
          p.uuid == AppProperties.notificationUuidToListen
      }
      .map {
         case (deviceID, p) =>
           deviceID -> p.value.toDouble
      }
      .groupByKey()
      .map {
        case (addr, v) =>
           addr -> breeze.stats.mean(v.toArray)
      }
      .updateStateByKey(updateFanStateIter, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
      .print()

    ssc.start()
    ssc.awaitTermination()
}
