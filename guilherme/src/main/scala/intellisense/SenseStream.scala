package intellisense

import org.apache.flink.streaming.api.scala._

object SenseStream {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.execute("Guilherme @ Intellisense")
  }
}
