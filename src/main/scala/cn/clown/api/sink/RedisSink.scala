package cn.clown.api.sink

import cn.clown.entity.SensorReading
import cn.clown.util.EnvironmentUtil
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSink {
  def main(args: Array[String]): Unit = {
    val util = new EnvironmentUtil()
    val stream: DataStream[SensorReading] = util.init_dataStream
    val conf = new FlinkJedisPoolConfig.Builder()
        .setHost("localhost")
        .setPort(6379)
        .build()


    stream.addSink(new RedisSink[SensorReading](conf,new MyRedisMapper()))
    util.execute
  }
}
class MyRedisMapper extends RedisMapper[SensorReading] {
  override def getCommandDescription: RedisCommandDescription = {
    val redisCommand = new RedisCommandDescription(RedisCommand.HSET, "sensor_temp")
    redisCommand
  }

  override def getKeyFromData(t: SensorReading): String = {
    t.id
  }

  override def getValueFromData(t: SensorReading): String = {
    t.temperature.toString
  }
}
