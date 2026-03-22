package kafka.telem.agg
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import play.api.libs.json._

class Http(store: TelemStorage) {
  private def getUriContext(query: String): (String, Long, String) = {
    val params: Map[String, String] =
      query
        .split("&")
        .toList
        .flatMap {
          case kv if kv.contains("=") =>
            val Array(k, v) = kv.split("=", 2)
            Some(k -> v)
          case _ => None
        }
        .toMap

    val deviceOpt = params.getOrElse("device", "")
    val operation = params.getOrElse("operation", "avg")
    val windowMsOpt = params.get("window").map(_.toLong).getOrElse(Config.default_search_ms)
    (deviceOpt, windowMsOpt * 1000, operation)
  }

  private def SendResponse(exchange: HttpExchange, json: JsObject): Unit = {
    val response = Json.stringify(json)
    val bytes = response.getBytes(StandardCharsets.UTF_8)

    exchange.getResponseHeaders.add("Content-Type", "application/json")

    exchange.sendResponseHeaders(200, bytes.length)

    val os = exchange.getResponseBody
    os.write(bytes)
    os.close()
  }

  def startServer(): Unit = {
    val server = HttpServer.create(new InetSocketAddress("0.0.0.0", Config.port), 0)

    server.createContext("/agg", new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {

        if (exchange.getRequestMethod != "GET") {
          exchange.sendResponseHeaders(405, -1)
          return
        }
        val query = Option(exchange.getRequestURI.getQuery).getOrElse("")
        val (incDevice, incWindowMs, operation) = getUriContext(query)
        if (incDevice == "") {
          exchange.sendResponseHeaders(400, -1)
          return
        }
        val result =
          if (operation == "avg") {
            store.average(windowMs = incWindowMs, deviceLookup = incDevice, nowMs = System.currentTimeMillis())
          } else if (operation == "median"){
            store.median(windowMs = incWindowMs, deviceLookup = incDevice, nowMs = System.currentTimeMillis())
          } else if (operation == "max") {
            store.max(windowMs = incWindowMs, deviceLookup = incDevice, nowMs = System.currentTimeMillis())
          } else if (operation == "min") {
            store.min(windowMs = incWindowMs, deviceLookup = incDevice, nowMs = System.currentTimeMillis())
          } else {
            exchange.sendResponseHeaders(400, -1)
            return
          }

        val count = store.count(windowMs = incWindowMs, deviceLookup = incDevice, nowMs = System.currentTimeMillis())

        val json = Json.obj(
          "average" -> result,
          "count" -> count
        )
        SendResponse(exchange, json)
      }
    }
    )



    server.start()
    println("Server running on http://localhost:8081/agg")
  }
}