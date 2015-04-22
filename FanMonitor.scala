package com.devicehive

import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import scala.util.Try

import dispatch._, Defaults._

object FanMonitor extends App {
    if (args.length < 3) {
      System.err.println(s"""
        |Usage: FanMonitor <brokers> <seconds> <url> [<threshold>]
        |  <brokers> is a list of one or more Kafka brokers
        |  <seconds> monitoring "window" length in seconds
        |  <url> service URL to track monitor state changes; monitor will send POST to this address with state parameter set to 1 (vibration) or 0 (no vibration)
        |  <threshold> vibration threshold (oscilation variance)
        |
        """.stripMargin)
      System.exit(1)
    }

    val brokers = args(0)
    val windowSize = args(1).toInt
    val service = url(args(2))
    val threshold = Try(args(3).toDouble).getOrElse(0.2D)

    var state = false

    val sc = new SparkConf().setAppName("FanMonitor")
    val ssc =  new StreamingContext(sc, Seconds(windowSize))

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set("device_notification"))

    val measurements = messages.filter(msg => msg._2.contains("""\"handle\":45""")).map {
      t =>
        val elements = t._2.split(",")

        val h = (":([0-9]*)".r findFirstMatchIn elements(5)).flatMap{ m => Some(java.lang.Integer.parseInt(m.group(1))) }.getOrElse(0)
        val v = (":[^\"]\"([0-9A-F]*)".r findFirstMatchIn elements(6)).flatMap{ m => Some(java.lang.Integer.parseInt(m.group(1), 16)) }.getOrElse(0)

        val x = (v & 0xFF).toByte / 64.0
        val y = ((v >> 8) & 0xFF).toByte / 64.0
        val z = (v >> 16).toByte * -1.0 / 64.0

        val dist = scala.math.sqrt(x*x + y*y + z*z)

        require(h == 45, "Unexpected handle on sensor data map")

        dist
    }
    .foreachRDD {
      rdd =>
        val variance = breeze.stats.meanAndVariance(rdd.toArray).variance

        val newState = if (variance > 0.11)
          true
        else
          false

        if (state != newState) {
          state = newState
          println(s"State changed! Now it is $state")
          val strState = if(state) "1" else "0"

          Http(service << Map("state" -> strState))
        }

        println(s"variance: $variance")
    }

    ssc.start()
    ssc.awaitTermination()
}
