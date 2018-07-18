package cn.xdf.wlyy.demo

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dongjunliang on 2018-6-27.
  */
object SparkSQLDemo {

    case class Dept(dept_id: Int, dept_name: String)

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("sparkSQLDemo")
        val sc = new SparkContext(conf)
        val sparkSession = SparkSession.builder().config(conf).getOrCreate()
        val deptRDD = sc.makeRDD(Seq(Dept(1, "Sales"), Dept(2, "HR")))
        val deptDS = sparkSession.createDataset(deptRDD)
        val deptDF = sparkSession.createDataFrame(deptRDD)
        deptDS.rdd
        deptDF.rdd
        deptDF.show()
    }
}
