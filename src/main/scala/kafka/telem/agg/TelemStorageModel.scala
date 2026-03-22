package kafka.telem.agg

case class TimedValue(timestampMs: Long, value: Double, device: String)
