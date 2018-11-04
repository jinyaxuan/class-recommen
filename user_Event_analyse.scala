package main.content_recall

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import com.google.gson.{JsonObject, JsonParser}
import content_recall.Tool_func._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

import content_recall.Tool_func.MergeData

object user_Event_analyse {

  /**
    *
    * @param context
    * @param date_val 时间范围
    * @param date_stamp 时间戳
    * @return
    */
  def cal_order_record(context: HiveContext, date_val:String, date_stamp:String)={
    import context.implicits._
    val data: DataFrame = context.sql("select * from ods.xes_stu_user_order " +
      "where sid is not null and sid != '-' and sid !='' " +
      "and finacreatedate > '2016-09-01' and dt"+ date_val).toDF()

    val data_2: RDD[(String, Iterable[Row])] = data.map{
      row => Tuple2(row.getAs("sid").toString,row)
    }.groupByKey

    val data_3: RDD[Row] = data_2.flatMap{
      row =>{
        val ret_rdd_arr =  new ArrayBuffer[Row]()
        val iter: Iterator[Row] = row._2.iterator
        val sid:String = row._1
        for(x<-iter){
          val price_weight:Double = cal_price(x.getAs[Byte]("orderstatus"),
            x.getAs[Double]("classprice"))
          val level = match_level(x.getAs("classlevelname"))
          val subject = match_subject(x.getAs("subjectname"))
          val order_Weight = order_status2weight(x.getAs[Byte]("orderstatus"))
          ret_rdd_arr.+=(Row(sid,subject+level,order_Weight)) //科目等级偏好
          ret_rdd_arr.+=(Row(sid,subject,2.0)) //科目偏好
          ret_rdd_arr.+=(Row(sid,subject+"price",price_weight)) //科目消费
        }
        ret_rdd_arr
      }
    }

    val ret_rdd: RDD[Row] = data_3.map{
      row => (Tuple2(row.getAs[String](0),row.getAs[String](1)),row.getAs[Double](2))
    }.reduceByKey(_+_)
      .map{
        row=>{
          Row(row._1._1,row._1._2,row._2)
        }
      }
    ret_rdd
  }

  /**
    * 统计科目等级偏好、科目偏好、科目班型偏好
    * @param context
    * @param date_val 需要传入=，<，>
    * @param date_stamp 时间戳
    * @return 基于日志行为计算，返回所有在date_val天有日志用户的科目偏好、科目等级偏好、科目班型偏好
    */

  def cal_event_record(context:HiveContext,date_val:String,date_stamp:String):RDD[Row] = {
    import context.implicits._
    context.sql("use ods").show()
    val event_data: DataFrame = context.sql("select * from ods.user_behavior " +
      "where cla_id is not null and cla_id != '' and cla_id != '-'" +
      "and sid is not null and sid != '-' and sid !='' and day "+ date_val).toDF()

    val semi_data: RDD[Row] = event_data.map{
      row =>Tuple2(row.getAs[String]("sid"),row)
    }.groupByKey
      .flatMap{
        row => {
          val Row_Iterator = row._2.iterator
          val Ret_Arr = new ArrayBuffer[Row]()
          val Sid = row._1.toString
          for(x<-Row_Iterator){
            val Cla_Level = match_level(x.getAs[String]("cla_level_name"))
            val Cla_Subject = match_subject(x.getAs[String]("cla_subject_names"))
            val Cla_Type = wash_cla_type(x.getAs[String]("cla_class_type"))
            Ret_Arr.+=(Row(Sid,Cla_Subject+Cla_Level,1.0)) //科目等级偏好
            Ret_Arr.+=(Row(Sid,Cla_Subject,1.0))  //科目偏好
            Ret_Arr.+=(Row(Sid,Cla_Subject+Cla_Type,1.0))//科目班型偏好 //双师、面授、在线
          }
          Ret_Arr
        }
      }
    val Plus_data: RDD[Row] = semi_data.map{
      row=>{
        (Tuple2(row.getAs[String](0),row.getAs[String](1)),row.getAs[Double](2))
      }
    }.reduceByKey(_+_)
      .map{
        row=> {
          Row(row._1._1,row._1._2,row._2)
        }
      }
    Plus_data
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("yarn-cluster").setAppName("Mym")
    val spark = new SparkContext(conf)
    val context = new HiveContext(spark)

    val schemaString="sid,feature_name,value,dt"
    val fields=schemaString.split(",").map(fieldName => StructField(fieldName, StringType, nullable = true))
    fields.update(2,StructField("value", DoubleType, nullable = true))
    val schema=StructType(fields)

    val fm = new SimpleDateFormat("yyyy-MM-dd")

    var EventUnionData: RDD[Row] = cal_event_record(context,"='2018-08-10'","2018-09-05")
    val EventStartDate = "2018-08-11"
    val EventEndDate = "2018-09-05"
    val EventStartDt = fm.parse(EventStartDate)
    val EventEndDt = fm.parse(EventEndDate)
    while(EventStartDt.compareTo(EventEndDt)<1){
      val RddToPlus: RDD[Row] = cal_event_record(context,"='"+fm.format(EventStartDt)+"'","2018-09-05")
      EventUnionData = MergeData(EventUnionData,RddToPlus,"2018-09-05")
      EventStartDt.setTime(EventStartDt.getTime + 24*3600*1000)
    }
    val EventContent = context.createDataFrame(EventUnionData,schema)
    EventContent.write.mode(SaveMode.Append).saveAsTable("tss.event_yarn_test")


    var OrderUnionData: RDD[Row] = cal_order_record(context,"='2018-08-01'","2018-09-05")
    val OrderStartDate = "2018-08-02"
    val OrderEndDate = "2018-09-05"
    val OrderStartDt = fm.parse(OrderStartDate)
    val OrderEndDt = fm.parse(OrderEndDate)
    while(OrderStartDt.compareTo(OrderEndDt)<1){
      val RddToPlus: RDD[Row] = cal_order_record(context,"='"+fm.format(OrderStartDt)+"'","2018-09-05")
      OrderUnionData = MergeData(OrderUnionData,RddToPlus,"2018-09-05")
      OrderStartDt.setTime(OrderStartDt.getTime + 24*3600*1000)
    }
    val OrderContent = context.createDataFrame(OrderUnionData,schema)
    OrderContent.write.mode(SaveMode.Append).saveAsTable("tss.order_yarn_test")


    val OrderEventData = MergeData(OrderUnionData,EventUnionData,"2018-09-05")
    val OrderEvent = context.createDataFrame(OrderEventData,schema)
    OrderEvent.write.mode(SaveMode.Append).saveAsTable("tss.orderevent_yarn_test")

    spark.stop()

  }

}
