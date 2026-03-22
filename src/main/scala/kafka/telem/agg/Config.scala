package kafka.telem.agg

import com.typesafe.config.ConfigFactory
import scala.util.Try

object Config {
  private val config = ConfigFactory.load()

  private def getIntEnv(name: String): Option[Int] =
    sys.env.get(name).flatMap(v => Try(v.toInt).toOption)

  private def getLongEnv(name: String): Option[Long] =
    sys.env.get(name).flatMap(v => Try(v.toLong).toOption)

  val port: Int =
    getIntEnv("SCALA_PORT").getOrElse(config.getInt("app.port"))

  val default_search_ms: Long =
    getLongEnv("SCALA_DEFAULT_SEARCH_S").getOrElse(config.getLong("app.default_search_s") * 1000L)

  val retentionMs: Long =
    getLongEnv("SCALA_RETENTION_MS").getOrElse(config.getLong("app.retention_ms"))

  val consumerGroup: String =
    sys.env.get("SCALA_CONSUMER_GROUP")
      .getOrElse(config.getString("app.consumer_group"))

  val kafkaServer: String =
    sys.env.get("KAFKA_BOOTSTRAP_SERVERS")
      .getOrElse(config.getString("app.kafka_server"))
}