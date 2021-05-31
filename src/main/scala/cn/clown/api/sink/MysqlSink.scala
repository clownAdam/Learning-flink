package cn.clown.api.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import cn.clown.entity.SensorReading
import cn.clown.util.EnvironmentUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala.DataStream

object MysqlSink {
  def main(args: Array[String]): Unit = {
    val demo: EnvironmentUtil = new EnvironmentUtil()
    val stream: DataStream[SensorReading] = demo.init_dataStream
    stream.addSink(new MyJdbcFunc())
    demo.execute
  }
}

class MyJdbcFunc extends RichSinkFunction[SensorReading] {
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql:///flink", "root", "root")
    insertStmt = conn.prepareStatement("insert into sensor_temp (id,temp) values(?,?)")
    updateStmt = conn.prepareStatement("update sensor_temp set temp=? where id=?")
  }

  override def invoke(value: SensorReading): Unit = {
    // 更新
    updateStmt.setString(1, value.temperature.toString)
    updateStmt.setString(2, value.id)
    updateStmt.execute()

    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.id)
      insertStmt.setString(2, value.temperature.toString)
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}