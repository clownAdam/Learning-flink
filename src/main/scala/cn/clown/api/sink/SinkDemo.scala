package cn.clown.api.sink

import cn.clown.entity.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._

object SinkDemo {
  def main(args: Array[String]): Unit = {
    var env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    var ds1: DataStream[String] = env.readTextFile("data/sensor.txt")
    var dataStream: DataStream[SensorReading] = ds1.map(data => {
      val arr: Array[String] = data.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })
    dataStream.print()

    dataStream.addSink(StreamingFileSink.forRowFormat(
      new Path("data/clown"),
      new SimpleStringEncoder[SensorReading]()
    ).build()).setParallelism(1)

    env.execute("fink sink")
  }
}
