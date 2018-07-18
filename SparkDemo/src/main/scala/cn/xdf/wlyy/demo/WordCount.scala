package cn.xdf.wlyy.demo

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dongjunliang on 2018-6-26.
  */
object WordCount {

    case class WordCount(word: String, count: Int)

    def Insert(iterator: Iterator[(String, Int)]): Unit = {
        var conn: Connection = null
        var ps: PreparedStatement = null
        val sql = "insert into wordcount(word, count) values (?, ?)"
        try {
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark_demo", "root", "root")
            iterator.foreach(data => {
                ps = conn.prepareStatement(sql)
                ps.setString(1, data._1)
                ps.setInt(2, data._2)
                ps.executeUpdate()
            })
        } catch {
            case e: Exception => println("MySQL Exception")
        } finally {
            if (ps != null) {
                ps.close()
            }
            if (conn != null) {
                conn.close()
            }
        }
    }

    def main(args: Array[String]) {
        //初始化SparkConf，包含了spark集群的各种参数
        val conf = new SparkConf().setMaster("local").setAppName("testRDD")
        //spark程序从SparkContext开始
        val sc = new SparkContext(conf)
        val data = sc.textFile("D:\\WorkSpace\\data\\log.txt")
        //flatMap是对行进行处理的方法,_是占位符
        data.flatMap(_.split(" "))
            .map((_, 1)) //将数据转换位key-value形式，数据是key，value是1
            .reduceByKey(_ + _) //把具有相同key的数据相加合并
            .foreachPartition(Insert)
    }
}
