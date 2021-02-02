package cn.clown.wc

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

/**
 * 批处理的wordcount
 *
 * @author clown
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    //1.创建一个批处理的执行环境
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //2.从文件中读取数据
    val inputPath: String = "data/hello.txt"

    //3.读取数据
    val inputDataSet: DataSet[String] = environment.readTextFile(inputPath)

    //4.对数据进行转换处理统计,先分词flatmap,再按照word进行分组,再次进行聚合统计
    val resultDataSet: AggregateDataSet[(String, Int)] = inputDataSet.flatMap(_.split(","))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    //5.打印输出
    resultDataSet.print()
  }
}
