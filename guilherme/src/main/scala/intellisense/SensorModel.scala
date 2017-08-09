package intellisense

import org.joda.time.DateTime
import org.numenta.nupic.Parameters.KEY
import org.numenta.nupic.algorithms.{Anomaly, SpatialPooler, TemporalMemory}
import org.numenta.nupic.encoders.scala.{DateEncoder, MultiEncoder, ScalarEncoder}
import org.numenta.nupic.flink.streaming.examples.common.NetworkDemoParameters
import org.numenta.nupic.network.Network

trait SensorModel {
  case class SensorData(timestamp: DateTime, data: Double)

  case class Prediction(timestamp: String, actual: Double, predicted: Double, anomalyScore: Double)

  val network = (key: AnyRef) => {
    val encoder = MultiEncoder(
      DateEncoder().name("timestamp").timeOfDay(21, 9.5),
      ScalarEncoder().name("data").n(50).w(21).maxVal(100.0).resolution(0.1).clipInput(true)
    )

    val params = NetworkDemoParameters.params

    val network = Network.create("Intellisense Network", params)
      .add(Network.createRegion("Region 1")
        .add(Network.createLayer("Layer 2/3", params)
          .alterParameter(KEY.AUTO_CLASSIFY, true)
          .add(encoder)
          .add(Anomaly.create())
          .add(new TemporalMemory())
          .add(new SpatialPooler())))

    network.setEncoder(encoder)
    network
  }
}