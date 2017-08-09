package intellisense

import java.net.{InetAddress, InetSocketAddress}

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink
import org.joda.time.DateTime
import org.numenta.nupic.encoders.DateEncoder._


object SenseStream {
  case class SensorData(timestamp: DateTime, data: Double)

  def main(args: Array[String]) {
    val path = args(0)
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Reading input from CSV as data stream
    val input: DataStream[SensorData] = env.readTextFile(path).map {
      _.split(",") match {
        case Array(timestamp, data) => SensorData(LOOSE_DATE_TIME.parseDateTime(timestamp), data.toDouble)
      }
    }

    // Anomaly detection


    // Output records into elasticsearch sink
    val config = new java.util.HashMap[String, String]
    config.put("cluster.name", "intellisense")
    config.put("bulk.flush.max.actions", "1")

    val transportAddresses = new java.util.ArrayList[InetSocketAddress]
    transportAddresses.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300))

    input.addSink(new ElasticsearchSink(config, transportAddresses, new SensorDataSinkFunction))

    env.execute("Guilherme @ Intellisense")
  }
}
