package main.content_recall

import java.io.{File, PrintWriter}

import akka.io.Udp.SO.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex
import content_recall.Tool_func.{match_level,match_subject,order_status2weight}

object Content_order_recall {



  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val spark = new SparkContext(conf)
    val context = new HiveContext(spark)
    context.sql("use ods").show()
    //val data = context.sql("select distinctid,event_json from event_stu_action where day = '2018-08-15'limit 30").toDF()
    val data: DataFrame = context.sql("select * from xes_stu_user_order where dt = '2018-08-17'").toDF()

    val data_2: RDD[(String, Iterable[Row])] = data.map(row => Tuple2(row.getAs("distinctid").toString,row)).groupByKey()

    val data_3: RDD[Row] = data_2.flatMap(row =>{

      val ret_rdd_arr =  new ArrayBuffer[Row]()
      val iter: Iterator[Row] = row._2.iterator

      for(x<-iter){

        val id:String = x.getAs[String]("distinctid")
        val claname = x.getAs[String]("classname")
        val price:Double = x.getAs[Double]("classprice")
        val lasstime = x.getAs[String]("lasstime")
        val startdate = x.getAs[String]("startdate")
        val grade = x.getAs[String]("gradename")
        val teacher = x.getAs[String]("teachername")
        val termname = x.getAs[String]("termname")

        val level = match_level(x.getAs("classlevelname"))
        val subject = match_subject(x.getAs("subjectname"))
        val order_Weight = order_status2weight(x.getAs[Byte]("orderstatus"))

        ret_rdd_arr.+=(Row(id,subject+level,order_Weight.toString,startdate))
        ret_rdd_arr.+=(Row(id,"price",price.toString,startdate))
      }
      ret_rdd_arr
    })

    //输出至本地磁盘
    data_3.coalesce(1,true).saveAsTextFile("E:/test_result")


    val schemaString="distinctid,feature_name,value,dt"
    val fields=schemaString.split(",").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema=StructType(fields)
    //写入hive表
    //context.createDataFrame(data_3,schema).write.saveAsTable("testdb.content_recall_new")

    val dataFrame = context.createDataFrame(data_3,schema)

    val ret: RDD[((String, String), Double)] =
      dataFrame.map(row => (Tuple2(row.getAs[String]("distinctid"),//new Tuple2
        row.getAs[String]("feature_name")),row.getAs[String]("value").toDouble)).
        reduceByKey(_+_)
    ret.foreach(x => println(x))

    spark.stop()

  }

}
