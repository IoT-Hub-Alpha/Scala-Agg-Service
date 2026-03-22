package kafka.telem.agg

import scala.collection.mutable.ArrayBuffer

class TelemStorage(retentionMs: Long) {
  private val values = ArrayBuffer.empty[TimedValue]

  def add(value: Double, device: String, timestampMs: Long): Unit = synchronized {
    values += TimedValue(timestampMs, value, device)
    evictOld(timestampMs)
  }

  def average(windowMs: Long, nowMs: Long, deviceLookup: String): Double = synchronized {
    evictOld(nowMs)

    val cutoff = nowMs - windowMs
    val inWindow = values.view.filter(v => v.timestampMs >= cutoff && v.device == deviceLookup)

    if (inWindow.isEmpty) 0.0
    else inWindow.map(_.value).sum / inWindow.size
  }

  def count(windowMs: Long, nowMs: Long, deviceLookup: String): Int = synchronized {
    evictOld(nowMs)

    val cutoff = nowMs - windowMs
    values.count(v => v.timestampMs >= cutoff && v.device == deviceLookup)
  }

  def max(windowMs: Long, nowMs: Long, deviceLookup: String): Double = synchronized {
    val cutoff = nowMs - windowMs

    val inWindow = values.view.filter(v => v.timestampMs >= cutoff && v.device == deviceLookup)

    if (inWindow.isEmpty) 0.0
    else inWindow.map(_.value).max
  }

  def min(windowMs: Long, nowMs: Long, deviceLookup: String): Double = synchronized {
    val cutoff = nowMs - windowMs

    val inWindow = values.view.filter(v => v.timestampMs >= cutoff && v.device == deviceLookup)

    if (inWindow.isEmpty) 0.0
    else inWindow.map(_.value).min
  }

  def median(windowMs: Long, nowMs: Long, deviceLookup: String): Double = synchronized {
    val cutoff = nowMs - windowMs

    val medianOpt = medianDoubles(
      values.view
        .filter(v => v.timestampMs >= cutoff && v.device == deviceLookup)
        .map(_.value)
        .toSeq
    ).getOrElse(0.0)

    medianOpt
  }

  private def evictOld(nowMs: Long): Unit = {
    val cutOff = nowMs - retentionMs

    var removeCount = 0
    while (removeCount < values.length && values(removeCount).timestampMs < cutOff) {
      removeCount += 1
    }

    if (removeCount > 0) {
      values.remove(0, removeCount)
    }
  }

  private def medianDoubles(xs: Seq[Double]): Option[Double] = {
    val sorted = xs.sorted
    val n = sorted.size

    if (n == 0) None
    else if (n % 2 == 1) Some(sorted(n / 2))
    else Some((sorted(n / 2 - 1) + sorted(n / 2)) / 2)
  }
}

