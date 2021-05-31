package cn.clown.api.transform

import cn.clown.entity.SensorReading
import org.apache.flink.api.common.functions.ReduceFunction

class SensorReduce extends ReduceFunction[SensorReading]{
  override def reduce(curState: SensorReading, newData: SensorReading): SensorReading = {
    SensorReading(curState.id, curState.timestamp.max(newData.timestamp), curState.temperature.min(newData.temperature))
  }
}
