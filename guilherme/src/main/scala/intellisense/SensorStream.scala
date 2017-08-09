package intellisense

import java.net.{InetAddress, InetSocketAddress}

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink
import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer
import org.apache.flink.streaming.api.scala._
import org.joda.time.DateTime
import org.numenta.nupic.encoders.DateEncoder._
import org.numenta.nupic.flink.streaming.api.scala._


object SensorStream extends SensorModel {

  def main(args: Array[String]) {
    val path = args(0)
    val env = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)
      env.addDefaultKryoSerializer(classOf[DateTime], classOf[JodaDateTimeSerializer])
      env
    }

    // Reading input from CSV as data stream
    val input: DataStream[SensorData] = env.readTextFile(path).map {
      _.split(",") match {
        case Array(timestamp, data) => SensorData(LOOSE_DATE_TIME.parseDateTime(timestamp), data.toDouble)
      }
    }

    // Anomaly detection
    val inferences: DataStream[Prediction] = input
      .learn(network)
      .select { inference => inference }
      .keyBy { _ => None }
      .mapWithState { (inference, state: Option[Double]) =>

        val prediction = Prediction(
          inference._1.timestamp.toString(LOOSE_DATE_TIME),
          inference._1.data,
          state match {
            case Some(prediction) => prediction
            case None => 0.0
          },
          inference._2.getAnomalyScore)

        // store the prediction about the next value as state for the next iteration,
        // so that actual vs predicted is a meaningful comparison
        val predictedData = inference._2.getClassification("data").getMostProbableValue(1).asInstanceOf[Any] match {
          case value: Double if value != 0.0 => value
          case _ => state.getOrElse(0.0)
        }

        (prediction, Some(predictedData))
      }


    // Output records into elasticsearch sink
    val config = new java.util.HashMap[String, String]
    config.put("cluster.name", "intellisense")
    config.put("bulk.flush.max.actions", "1")

    val transportAddresses = new java.util.ArrayList[InetSocketAddress]
    transportAddresses.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300))

    inferences.addSink(new ElasticsearchSink(config, transportAddresses, new SensorDataSinkFunction))

    env.execute("Guilherme @ Intellisense")
  }
}