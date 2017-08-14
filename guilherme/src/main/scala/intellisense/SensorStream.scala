package intellisense

import java.net.{InetAddress, InetSocketAddress}

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink
import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer
import org.apache.flink.streaming.api.scala._
import org.joda.time.DateTime
import org.numenta.nupic.Parameters.KEY
import org.numenta.nupic.algorithms.{Anomaly, SpatialPooler, TemporalMemory}
import org.numenta.nupic.encoders.scala.{DateEncoder, MultiEncoder, ScalarEncoder}
import org.numenta.nupic.flink.streaming.api.scala._
import org.numenta.nupic.flink.streaming.examples.common.NetworkDemoParameters
import org.numenta.nupic.network.Network

object SensorStream extends SensorModel {

  def parseValue(v: String): Double = {
    if (v == "") {
      0.0
    } else {
        try {
          v.toDouble
        } catch {
          case _: Throwable => 0.0
        }
    }
  }

  def main(args: Array[String]) {
    var path = ""
    var cluster = ""
    var ip = ""
    var port = 0
    args.sliding(2, 2).toList.collect {
      case Array("--path", argPath: String) => path = argPath
      case Array("--cluster", argCluster: String) => cluster = argCluster
      case Array("--ip", argIP: String) => ip = argIP
      case Array("--port", argPort: String) => port = argPort.toInt
    }
    val env = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)
      env.addDefaultKryoSerializer(classOf[DateTime], classOf[JodaDateTimeSerializer])
      env
    }

    // Reading input from CSV as data stream
    var header = true
    val input: DataStream[SensorData] = env.readTextFile(path).map(
      line => {
        var values = line.split(',')
        while(values.length < classOf[SensorData].getDeclaredFields.length - 1) {
          values = values :+ ""
        }
        values
      })
      .filter {
        values => {
          val isValue = !header
          header = false
          isValue
        }
      }
      .map { values => {SensorData(values(0).toInt,
                                   DateTime.parse(values(1)),
                                   parseValue(values(2)),
                                   parseValue(values(3)),
                                   parseValue(values(4)),
                                   parseValue(values(5)),
                                   parseValue(values(6)),
                                   parseValue(values(7)),
                                   parseValue(values(8)),
                                   parseValue(values(9)),
                                   parseValue(values(10)),
                                   parseValue(values(11)),
                                   parseValue(values(12)),
                                   parseValue(values(13)),
                                   parseValue(values(14)),
                                   parseValue(values(15)),
                                   parseValue(values(16)),
                                   parseValue(values(17)))
      }
      }


    // Configuring elasticsearch sink
    val config = new java.util.HashMap[String, String]
    config.put("cluster.name", cluster)
    config.put("bulk.flush.max.actions", "1")

    val transportAddresses = new java.util.ArrayList[InetSocketAddress]
    transportAddresses.add(new InetSocketAddress(InetAddress.getByName(ip), port))


    // Anomaly detection
    for (field <- fields) {
      val predictions: DataStream[Prediction] = input
        .learn((key: AnyRef) => {
          val encoder = MultiEncoder(
            DateEncoder().name("timestamp").timeOfDay(21, 9.5),
            ScalarEncoder().name(field).n(50).w(21).minVal(Double.MinValue).maxVal(Double.MaxValue).resolution(0.1).clipInput(true)
          )

          val params = NetworkDemoParameters.params

          val network = Network.create("Intellisense Network - " + field, params)
            .add(Network.createRegion("Region 1")
              .add(Network.createLayer("Layer 2/3", params)
                .alterParameter(KEY.AUTO_CLASSIFY, true)
                .add(encoder)
                .add(Anomaly.create())
                .add(new TemporalMemory())
                .add(new SpatialPooler())))

          network.setEncoder(encoder)
          network
        })
        .select { inference => inference }
        .keyBy { _ => None }
        .mapWithState { (inference, state: Option[Double]) =>

          val prediction = Prediction(
            inference._1.timestamp.toString,
            field,
            inference._1.getClass.getMethods.find(_.getName == field).get.invoke(inference._1),
            state match {
              case Some(predictedValue) => predictedValue
              case None => 0.0
            },
            inference._2.getAnomalyScore)

          // store the prediction about the next value as state for the next iteration,
          // so that actual vs predicted is a meaningful comparison
          val predictedData = inference._2.getClassification(field).getMostProbableValue(1).asInstanceOf[Any] match {
            case value: Double if value != 0.0 => value
            case _ => state.getOrElse(0.0)
          }

          (prediction, Some(predictedData))
        }
      predictions.addSink(new ElasticsearchSink(config, transportAddresses, new SensorDataSinkFunction))
    }

    env.execute("Guilherme @ Intellisense")
  }
}