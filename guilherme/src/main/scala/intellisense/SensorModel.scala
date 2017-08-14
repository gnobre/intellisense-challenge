package intellisense

import org.joda.time.DateTime
import org.numenta.nupic.Parameters.KEY
import org.numenta.nupic.algorithms.{Anomaly, SpatialPooler, TemporalMemory}
import org.numenta.nupic.encoders.scala.{DateEncoder, MultiEncoder, ScalarEncoder}
import org.numenta.nupic.flink.streaming.examples.common.NetworkDemoParameters
import org.numenta.nupic.network.Network

trait SensorModel {
  val fields: List[String] = List("bed_height", "bed_pressure", "clear_water_height",
  "feed_solids_mass_flow", "feed_valve_A", "feed_valve_B", "flocculant_dilution_flow_rate",
  "flocculant_flow_rate", "interface_height", "mud_height", "rake_height", "rake_torque",
  "underflow_flow_rate", "underflow_line_1_pump_running", "underflow_line_2_pump_running",
  "underflow_percent_solids")

  case class SensorData(index: Int,
                        timestamp: DateTime,
                        bed_height: Double,
                        bed_pressure: Double,
                        clear_water_height: Double,
                        feed_solids_mass_flow: Double,
                        feed_valve_A: Double,
                        feed_valve_B: Double,
                        flocculant_dilution_flow_rate: Double,
                        flocculant_flow_rate: Double,
                        interface_height: Double,
                        mud_height: Double,
                        rake_height: Double,
                        rake_torque: Double,
                        underflow_flow_rate: Double,
                        underflow_line_1_pump_running: Double,
                        underflow_line_2_pump_running: Double,
                        underflow_percent_solids: Double
                       )

  case class Prediction(timestamp: String,
                        type_es: String,
                        actual: Any,
                        predicted: Double,
                        anomalyScore: Double)
}