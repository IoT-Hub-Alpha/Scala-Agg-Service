package kafka.telem.agg
import java.time.{Duration, Instant}
import java.util.Properties
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import play.api.libs.json.Json

import scala.jdk.CollectionConverters._

class KafkaConsumerService(store: TelemStorage) {
  // 1. Kafka config
  val props = new Properties()
  props.put("bootstrap.servers", Config.kafkaServer)
  props.put("group.id", Config.consumerGroup)
  props.put("key.deserializer", classOf[StringDeserializer].getName)
  props.put("value.deserializer", classOf[StringDeserializer].getName)
  props.put("auto.offset.reset", "earliest") // or "latest"
  props.put("enable.auto.commit", "true")

  // 2. Create consumer
  private val consumer = new KafkaConsumer[String, String](props)

  // 3. Subscribe to topic
  consumer.subscribe(java.util.Collections.singletonList("telemetry.clean"))
  println("🚀 Listening to telemetry.clean...")

  // 4. Poll loop
  def run(): Unit = {
    while (true) {
      val records = consumer.poll(Duration.ofMillis(1000))

      for (record <- records.asScala) {
        val json = Json.parse(record.value())

        val serialOpt = (json \ "serial_number").asOpt[String]
        val valueOpt = (json \ "payload" \ "value").asOpt[Double]
        val timestampOpt = (json \ "payload" \ "timestamp").asOpt[String]

        (serialOpt, valueOpt, timestampOpt) match {
          case (Some(serial), Some(value), Some(timestamp)) =>
            println(s"Device: $serial | Value: $value | Timestamp: $timestamp")
            store.add(timestampMs = System.currentTimeMillis(), device = serial, value = value)

          case _ =>
            println(s"❌ Failed to parse message: ${record.value()}")
        }
      }
    }
  }
}

