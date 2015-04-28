package com.devicehive

import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import scala.util.Try

import org.json4s._
import org.json4s.jackson.JsonMethods._

import dispatch._, Defaults._

case class NotificationParams(address: String, characteristic: String, `type`: String, value: String)

object FanMonitor extends App {

    implicit val formats = DefaultFormats

    if (args.length < 5) {
      System.err.println(s"""
        |Usage: FanMonitor <brokers> <seconds> <url> <threshold> <characteristic>
        |  <brokers> is a list of one or more Kafka brokers
        |  <seconds> monitoring "window" length in seconds
        |  <url> service URL to track monitor state changes; monitor will send POST to this address with state parameter set to 1 (vibration) or 0 (no vibration)
        |  <threshold> vibration threshold (oscilation variance), defaults to 0.2
        |
        """.stripMargin)
      System.exit(1)
    }

    val brokers = args(0)
    val windowSize = args(1).toInt

    val service = url(args(2))
    val threshold = Try(args(3).toDouble).getOrElse(0.2D)
    val characteristic = args(4)

    var state = false

    val sc = new SparkConf().setAppName("FanMonitor")
    val ssc =  new StreamingContext(sc, Seconds(windowSize))

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set("device_notification"))

    /* Message sample:
       {"id":2078486905,"notification":"Notification","deviceGuid":"903D08A6-05D3-4571-9D1D-C6A1B8AC21EC",
        "timestamp":"2015-04-27T16:22:35.174235",
        "parameters":{"address":"e4c95cc8bbc1","characteristic":"299d64102f6111e281c10800200c9a66",
                      "type":"Indication","value":"8d501700815017004800000000000000"}
        "deviceId":"903D08A6-05D3-4571-9D1D-C6A1B8AC21EC"}
     */

    val measurements = messages
      .map {
        msg =>
          val jsonMsg = parse(msg._2)
          val jsonParams = parse((jsonMsg \\ "parameters").extract[String])

          jsonParams.extract[NotificationParams]
      }
      .filter(_.characteristic == characteristic)
      .map {
        p: NotificationParams =>
          val value = java.lang.Long.parseLong(p.value, 16)
          val x = (value & 0xFF).toByte / 64.0
          val y = ((value >> 8) & 0xFF).toByte / 64.0
          val z = (value >> 16).toByte * -1.0 / 64.0

          val dist = scala.math.sqrt(x*x + y*y + z*z)

          p.address -> dist
      }
      .groupByKey()
      .map {
        case (addr, v) =>
           addr -> breeze.stats.meanAndVariance(v.toArray).variance
      }
      .map {
        case (address, variance) =>

          val newState = if (variance > threshold)
            true
          else
            false

          if (state != newState) {
            state = newState
            println(s"State changed! Now it is $state")
            val strState = if(state) "0" else "1"

            // TODO: need to send device address also in post paramters
            Http(service << strState <:< Map("Content-Type" -> "text/plain"))
          }

          println(s"Fan vibraion variance: $variance / $threshold")
       }

    ssc.start()
    ssc.awaitTermination()
}
