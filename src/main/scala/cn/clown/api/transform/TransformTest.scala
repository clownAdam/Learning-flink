package cn.clown.api.transform

import cn.clown.entity.SensorReading
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.junit.Test

class TransformTest {
  //  @Test
  //  def testTransform(): Unit = {
  //    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  //    val ds: DataStream[String] = env.readTextFile("data/sensor.txt")
  //    val dataStream: DataStream[SensorReading] = ds.map(data => {
  //      val arr: Array[String] = data.split(",")
  //      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
  //    })
  //    /*val aggStream: DataStream[SensorReading] = dataStream.keyBy("id")
  //      .minBy("temperature")
  //    aggStream.print()*/
  //    //当前最小的温度值，最近的时间戳
  //    var resultStream: DataStream[SensorReading] = dataStream.keyBy("id")
  //      /*.reduce((curState, newData) => {
  //        SensorReading(curState.id, curState.timestamp.max(newData.timestamp), curState.temperature.min(newData.temperature))
  //      }*/
  //      .reduce(new SensorReduce())
  //    resultStream.print()
  //    env.execute()
  //  }

  /**
   * 分流：
   * 将传感器温度分成低温高温两条流
   */
  @Test
  def testTransform2(): Unit = {
    var env1: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    var ds1: DataStream[String] = env1.readTextFile("data/sensor.txt")
    var dataStream: DataStream[SensorReading] = ds1.map(data => {
      val arr: Array[String] = data.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })
    val splitStream: SplitStream[SensorReading] = dataStream.split(data => {
      if (data.temperature > 30.0) Seq("high") else Seq("low")
    })
    val high: DataStream[SensorReading] = splitStream.select("high")
    val low: DataStream[SensorReading] = splitStream.select("low")
    val all: DataStream[SensorReading] = splitStream.select("high","low")
    high.print("high")
    low.print("low")
    all.print("all")
    env1.execute()
  }
}
