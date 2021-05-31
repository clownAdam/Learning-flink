package cn.clown.api.source

import cn.clown.entity.SensorReading
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.collection.immutable
import scala.util.Random

class SensorSource extends SourceFunction[SensorReading] {
  var running: Boolean = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 随机数发生器
    val rand = new Random()
    //温度:id,temp
    var curTemp: immutable.IndexedSeq[(String, Double)] = 1.to(10).map(i => ("sensor_" + i, rand.nextDouble() * 100))
    while (running) {
      curTemp = curTemp.map(
        data => (data._1, data._2 + rand.nextGaussian())
      )
      val curTime: Long = System.currentTimeMillis()
      curTemp.foreach(
        data => sourceContext.collect(SensorReading(data._1, curTime, data._2))
      )
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}
