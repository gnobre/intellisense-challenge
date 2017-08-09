package intellisense

import intellisense.SensorStream.SensorData
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

class SensorDataSinkFunction extends ElasticsearchSinkFunction[SensorData] {

  def process(record: SensorData, ctx: RuntimeContext, indexer: RequestIndexer) {

    val json = new java.util.HashMap[String, String]
    json.put("data", record.data.toString)

    val request: IndexRequest = Requests.indexRequest
      .index("flink")
      .`type`("flink")
      .source(json)

    indexer.add(request)
  }
}
