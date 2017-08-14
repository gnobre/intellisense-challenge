package intellisense

import intellisense.SensorStream.Prediction
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

class SensorDataSinkFunction extends ElasticsearchSinkFunction[Prediction] {

  def process(record: Prediction, ctx: RuntimeContext, indexer: RequestIndexer) {

    val json = new java.util.HashMap[String, String]
    json.put("timestamp", record.timestamp)
    json.put("actual", record.actual.toString)
    json.put("predicted", record.predicted.toString)
    json.put("anomaly_score", record.anomalyScore.toString)

    val request: IndexRequest = Requests.indexRequest
      .index("sensor_data")
      .`type`(record.type_es)
      .source(json)

    indexer.add(request)
  }
}
