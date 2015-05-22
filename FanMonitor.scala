package com.devicehive

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{ SparkConf, HashPartitioner }
import scala.util.Try

import org.json4s._
import org.json4s.jackson.JsonMethods._

import dispatch._, Defaults._

case class Notification(id: Int, notification: String, deviceGuid: String,
                        timestamp: String, parameters: String, value: String, deviceId: String)
case class NotificationParams(mac: String, uuid: String, value: String)

case class FanState(initialized: Boolean, vibration: Boolean)

object FanMonitor extends App {

    implicit val formats = DefaultFormats

    if (args.length < 4) {
      System.err.println(s"""
        |Usage: FanMonitor <brokers> <seconds> <url> <threshold> <characteristic>
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

    val service = url(args(2))
    val threshold = Try(args(3).toDouble).getOrElse(0.2D)

    var state = false

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
           {"jsonString":"{\"mac\":\"bc6a29abd973\",\"uuid\":\"F000AA1104514000b000000000000000\",\"value\":\"ce3ce6\"}"}}
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
     * RED 0f0d0300ff00006400000000000067ffff
     * GREEN 0f0d030000ff006400000000000067ffff
     */

     def lampValue(vibration: Boolean) =
       if (vibration)
         "0f0d0300ff00006400000000000067ffff"
       else
         "0f0d030000ff006400000000000067ffff"

  val updateFanStateIter = (iter: Iterator[((String, String), Seq[Double], Option[FanState])]) =>
      iter.map {
        case ((deviceID, mac), vals, oldState) =>
          val state: FanState = oldState.getOrElse(FanState(false, false))

          val initialized = if (!state.initialized) {
            println("!!! device initialized !!!")
            println(s"""DH: ${service.toString}/device/$deviceID/command, { "timestamp": 1431098595709, "parameters":{"type":"DELIGHT", "mac":"$mac"},"command":"init" }""")
            true
          } else
            state.initialized

          val vibration = vals.exists(_ > threshold)

          if (state.vibration != vibration) {
            println("!!! vibration state changed !!!")
            println(s"""DH: ${service.toString}/device/$deviceID/command, { "timestamp": 1431098595709, "parameters":{"mac": "$mac", "uuid":"fff3", "value": "${lampValue(vibration)}"}, "command": "gatt/write" }""")
          }

          (deviceID, mac) -> FanState(initialized, vibration)
      }

    val measurements = messages
      .filter {
        msg =>
          val jsonMsg = parse(msg._2)

          (jsonMsg \\ "notification").extract[String] == "NotificationReceived"
      }
      .map {
        msg =>
          val jsonMsg = parse(msg._2)

          val deviceID= (jsonMsg \\ "deviceGuid").extract[String]
          val notification = ((jsonMsg \\ "parameters") \\ "jsonString").extract[String]
          val jsonParams = parse(notification)

          (deviceID -> jsonParams.extract[NotificationParams])
      }
      .map {
         case (deviceID, p) =>
          val value = java.lang.Long.parseLong(p.value, 16)
          val x = (value & 0xFF).toByte / 64.0
          val y = ((value >> 8) & 0xFF).toByte / 64.0
          val z = (value >> 16).toByte * -1.0 / 64.0

          val dist = scala.math.sqrt(x*x + y*y + z*z)

           (deviceID, p.mac) -> dist
      }
      .groupByKey()
      .map {
        case (addr, v) =>
           addr -> breeze.stats.meanAndVariance(v.toArray).variance
      }
      .updateStateByKey(updateFanStateIter, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
      .print()

    ssc.start()
    ssc.awaitTermination()
}
