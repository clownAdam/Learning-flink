package cn.clown.api.sink

import java.net.InetAddress
import java.util

import cn.clown.entity.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

object ElasticSink {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    var ds1: DataStream[String] = environment.readTextFile("data\\sensor.txt")
    var stream: DataStream[SensorReading] = ds1.map(data => {
      val arr: Array[String] = data.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })
    val address: InetAddress = InetAddress.getByName("192.168.233.77")
    val httpHost = new HttpHost(address, 9200)
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(httpHost)

    val elasticsearchSinkFunction = new myElasticSearchSink()

    stream.addSink(new ElasticsearchSink
    .Builder[SensorReading](httpHosts, elasticsearchSinkFunction)
      .build()
    )
    environment.execute("hello")
  }
}

class myElasticSearchSink extends ElasticsearchSinkFunction[SensorReading]() {
  /**
   *
   * @param element
   * @param ctx     上下文
   * @param indexer http请求发送数据
   */
  override def process(element: SensorReading, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
    // 包装一个es
    val dataSource = new util.HashMap[String, String]()
    dataSource.put("id", element.id)
    dataSource.put("temperature", element.temperature.toString)
    dataSource.put("ts", element.temperature.toString)

    val indexRequest: IndexRequest = Requests.indexRequest()
      .index("sensor")
      .`type`("readingdata")
      .source(dataSource)

    indexer.add(indexRequest)
  }
}
