package cn.clown.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //1.创建流处理的执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //从外部命令中提取参数,作为socket主机名和端口号
    val parameterTool: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = parameterTool.get("host")
    val port: Int = parameterTool.getInt("port")

    //设置全局cpu并行度
    //    environment.setParallelism(4)

    //2.接收socket文本流
    val inputDataStream: DataStream[String] = environment.socketTextStream(host, port)

    //3.进行转换处理统计
    val result: DataStream[(String, Int)] = inputDataStream.flatMap(_.split(","))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0) //相同key(基于key的hashcode进行取模)分到相同任务
      .sum(1)

    //4.打印输出
    result.print().setParallelism(1)

    //5.启动任务执行
    environment.execute("streamWordCount")
  }
}
