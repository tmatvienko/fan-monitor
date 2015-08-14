import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

import com.lambdaworks.jacks.JacksMapper

val ssc = new StreamingContext(sc, Seconds(2))

val messages = KafkaUtils.createStream(ssc, "52.0.124.231:2181", "my-consumer-group", Map("device_notification" -> 1))
val msgs = messages.window(Seconds(30))

case class ParsedNotification(deviceGuid: String, notification: String, timestamp: String, parameters: String)
case class Notification(deviceGuid: String, timestamp: String, mac: String, uuid:String, value: Double)

msgs.map(
  msg => JacksMapper.readValue[Map[String, Any]](msg._2)
).map(
    notificationMap => ParsedNotification(
      notificationMap.get("deviceGuid").get.asInstanceOf[String],
      notificationMap.get("notification").get.asInstanceOf[String],
      notificationMap.get("timestamp").get.asInstanceOf[String],
      notificationMap.get("parameters").get.asInstanceOf[Map[String, String]].getOrElse("jsonString", "{}"))
  ).filter(
    parsed => parsed.notification == "NotificationReceived"
  ).map(
    nmap => (nmap.deviceGuid, nmap.timestamp, JacksMapper.readValue[Map[String, Any]](nmap.parameters))
  ).filter(
    nmap => nmap._3.get("uuid").get.asInstanceOf[String] == "f000aa1104514000b000000000000000"
  ).map(
    x => Notification(x._1, x._2.substring(11,19),
      x._3.get("mac").get.asInstanceOf[String],
      x._3.get("uuid").get.asInstanceOf[String],
      x._3.get("value").get.asInstanceOf[Double])
  ).foreachRDD( rdd=>
  rdd.toDF().registerTempTable("notifications")
  )

ssc.start()

//%sql select value, timestamp from notifications where value > ${threshold=0.2}