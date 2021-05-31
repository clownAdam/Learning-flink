package cn.clown.api.source

import cn.clown.entity.SensorReading
import org.apache.flink.streaming.api.scala._
import org.junit.Test

class SourceTest {
  @Test
  def testSource(): Unit = {
    //创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //从集合中读取数据
    val dataList: List[SensorReading] = List(
      SensorReading("sensor_1", 1547718199, 35.1),
      SensorReading("sensor_2", 1547718199, 35.2),
      SensorReading("sensor_3", 1547718199, 35.3),
      SensorReading("sensor_4", 1547718199, 35.4)
    )
    //    val stream: DataStream[SensorReading] = env.fromCollection(dataList)
    //    stream.print()

    //    val value: DataStream[Any] = env.fromElements("f", 2, 1.3)
    //    value.print()

    //    env.readTextFile("data/sensor.txt").print()

    //    var properties = new Properties()
    //    properties.setProperty("bootstrap.servers","82.156.1.124:9092")
    //    properties.setProperty("group.id","consumer-group")
    //    env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(),properties)).print()

    env.addSource(new SensorSource).print()

    env.execute()
  }
}
