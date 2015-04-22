package com.devicehive

import java.util.Properties

import kafka.producer._

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf

object FanDataKafkaProducer extends App {

    if (args.length < 3) {
      System.err.println("Usage: FanDataKafkaProducer <brokers> <topic> " +
        "<messagesPerSec>")
      System.exit(1)
    }

    val broker = args(0)
    val topic = args(1)
    val mps = args(2).toInt

    val props = new Properties()
    props.put("metadata.broker.list", broker)
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)

    val msgTemplate = """snappy-demo2,136465295220870407,xgatt/value,"{\"device\":\"B4:99:4C:64:23:F7\",\"handle\":48,\"valueHex\":\"%s\"}",%s"""
    val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")

    while(true) {
      val messages = (1 to mps).map {
        n =>
       val str = msgTemplate.format(scala.util.Random.nextInt(10).toString, dateFormat.format(new java.util.Date()))

        new KeyedMessage[String, String](topic, str)
      }.toArray

      producer.send(messages: _*)
      Thread.sleep(1000)
    }
}
