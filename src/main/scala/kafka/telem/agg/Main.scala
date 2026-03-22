package kafka.telem.agg

object Main{
  def main(args: Array[String]): Unit = {
    val store = new TelemStorage(retentionMs = Config.retentionMs)
    val runKafkaConsumer = new KafkaConsumerService(store)
    val kafkaConsumerThread = new Thread(() => runKafkaConsumer.run())
    kafkaConsumerThread.setName("kafka-consumer")
    kafkaConsumerThread.setDaemon(true)
    kafkaConsumerThread.start()

    val httpServer = new Http(store)
    httpServer.startServer()

    println("App started")

    Thread.currentThread().join()
  }
}

