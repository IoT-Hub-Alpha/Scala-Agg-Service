package kafka.telem.agg

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TelemStorageSpec extends AnyFunSuite with Matchers {

  test("average returns 0.0 when there are no matching values") {
    val store = new TelemStorage(retentionMs = 60_000)

    store.average(windowMs = 10_000, nowMs = 50_000, deviceLookup = "device-1") shouldBe 0.0
  }

  test("count returns 0 when there are no matching values") {
    val store = new TelemStorage(retentionMs = 60_000)

    store.count(windowMs = 10_000, nowMs = 50_000, deviceLookup = "device-1") shouldBe 0
  }

  test("add + average + count work for a single device") {
    val store = new TelemStorage(retentionMs = 60_000)

    store.add(value = 10.0, device = "device-1", timestampMs = 1_000)
    store.add(value = 20.0, device = "device-1", timestampMs = 2_000)
    store.add(value = 30.0, device = "device-1", timestampMs = 3_000)

    store.count(windowMs = 10_000, nowMs = 5_000, deviceLookup = "device-1") shouldBe 3
    store.average(windowMs = 10_000, nowMs = 5_000, deviceLookup = "device-1") shouldBe 20.0
  }

  test("device filter isolates values by serial number") {
    val store = new TelemStorage(retentionMs = 60_000)

    store.add(value = 10.0, device = "device-1", timestampMs = 1_000)
    store.add(value = 20.0, device = "device-2", timestampMs = 2_000)
    store.add(value = 30.0, device = "device-1", timestampMs = 3_000)

    store.count(windowMs = 10_000, nowMs = 5_000, deviceLookup = "device-1") shouldBe 2
    store.average(windowMs = 10_000, nowMs = 5_000, deviceLookup = "device-1") shouldBe 20.0

    store.count(windowMs = 10_000, nowMs = 5_000, deviceLookup = "device-2") shouldBe 1
    store.average(windowMs = 10_000, nowMs = 5_000, deviceLookup = "device-2") shouldBe 20.0
  }

  test("window filter excludes old values from average and count") {
    val store = new TelemStorage(retentionMs = 60_000)

    store.add(value = 10.0, device = "device-1", timestampMs = 1_000)
    store.add(value = 20.0, device = "device-1", timestampMs = 8_000)
    store.add(value = 30.0, device = "device-1", timestampMs = 9_000)

    store.count(windowMs = 2_000, nowMs = 10_000, deviceLookup = "device-1") shouldBe 2
    store.average(windowMs = 2_000, nowMs = 10_000, deviceLookup = "device-1") shouldBe 25.0
  }

  test("max returns maximum in the window") {
    val store = new TelemStorage(retentionMs = 60_000)

    store.add(value = 15.0, device = "device-1", timestampMs = 1_000)
    store.add(value = 7.0, device = "device-1", timestampMs = 2_000)
    store.add(value = 42.0, device = "device-1", timestampMs = 3_000)

    store.max(windowMs = 10_000, nowMs = 5_000, deviceLookup = "device-1") shouldBe 42.0
  }

  test("min returns minimum in the window") {
    val store = new TelemStorage(retentionMs = 60_000)

    store.add(value = 15.0, device = "device-1", timestampMs = 1_000)
    store.add(value = 7.0, device = "device-1", timestampMs = 2_000)
    store.add(value = 42.0, device = "device-1", timestampMs = 3_000)

    store.min(windowMs = 10_000, nowMs = 5_000, deviceLookup = "device-1") shouldBe 7.0
  }

  test("max and min return 0.0 when there are no matching values") {
    val store = new TelemStorage(retentionMs = 60_000)

    store.max(windowMs = 10_000, nowMs = 5_000, deviceLookup = "missing-device") shouldBe 0.0
    store.min(windowMs = 10_000, nowMs = 5_000, deviceLookup = "missing-device") shouldBe 0.0
  }

  test("median returns middle value for odd number of samples") {
    val store = new TelemStorage(retentionMs = 60_000)

    store.add(value = 30.0, device = "device-1", timestampMs = 1_000)
    store.add(value = 10.0, device = "device-1", timestampMs = 2_000)
    store.add(value = 20.0, device = "device-1", timestampMs = 3_000)

    store.median(windowMs = 10_000, nowMs = 5_000, deviceLookup = "device-1") shouldBe 20.0
  }

  test("median returns average of the two middle values for even number of samples") {
    val store = new TelemStorage(retentionMs = 60_000)

    store.add(value = 10.0, device = "device-1", timestampMs = 1_000)
    store.add(value = 20.0, device = "device-1", timestampMs = 2_000)
    store.add(value = 30.0, device = "device-1", timestampMs = 3_000)
    store.add(value = 40.0, device = "device-1", timestampMs = 4_000)

    store.median(windowMs = 10_000, nowMs = 5_000, deviceLookup = "device-1") shouldBe 25.0
  }

  test("median returns 0.0 when there are no matching values") {
    val store = new TelemStorage(retentionMs = 60_000)

    store.median(windowMs = 10_000, nowMs = 5_000, deviceLookup = "missing-device") shouldBe 0.0
  }

  test("retention eviction removes values older than retention window") {
    val store = new TelemStorage(retentionMs = 5_000)

    store.add(value = 10.0, device = "device-1", timestampMs = 1_000)
    store.add(value = 20.0, device = "device-1", timestampMs = 4_000)
    store.add(value = 30.0, device = "device-1", timestampMs = 7_000)

    // At add(timestamp=7000), cutoff is 2000, so value at 1000 should be evicted.
    store.count(windowMs = 10_000, nowMs = 7_000, deviceLookup = "device-1") shouldBe 2
    store.average(windowMs = 10_000, nowMs = 7_000, deviceLookup = "device-1") shouldBe 25.0
  }

  test("old values can also be evicted during read operations") {
    val store = new TelemStorage(retentionMs = 5_000)

    store.add(value = 10.0, device = "device-1", timestampMs = 1_000)
    store.add(value = 20.0, device = "device-1", timestampMs = 2_000)
    store.add(value = 30.0, device = "device-1", timestampMs = 3_000)

    // cutoff = 10_000 - 5_000 = 5_000, so everything should be evicted
    store.count(windowMs = 10_000, nowMs = 10_000, deviceLookup = "device-1") shouldBe 0
    store.average(windowMs = 10_000, nowMs = 10_000, deviceLookup = "device-1") shouldBe 0.0
  }
}