package cn.clown.util

import cn.clown.entity.SensorReading
import org.apache.flink.streaming.api.scala._

class EnvironmentUtil {
  var env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  def init_dataStream: DataStream[SensorReading] = {
    var ds1: DataStream[String] = env.readTextFile("data/sensor.txt")
    var dataStream: DataStream[SensorReading] = ds1.map(data => {
      val arr: Array[String] = data.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })
    dataStream
  }

  def execute: Unit = {
    env.execute("execute")
  }
}
