package content_recall

import com.google.gson.{JsonObject, JsonParser}
import content_recall.Tool_func.{match_level, match_subject, order_status2weight,cal_price,wash_cla_type}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json.JSONObject

object user_Event_analyse_local {
/*
  def cal_list_output(context: HiveContext) = {
    import context.implicits._
    val raw_data = context.sql("select sid,appversion,event_json from testdb.event_stu_action_test " +
      "where eventtype = 'browse_detail'").toDF()

    println("===================raw_data========================")
    //raw_data.foreach(x=>println(x))
    val data_2: RDD[(String, Iterable[Row])] = raw_data.filter($"sid".notEqual("")).map { row =>
      (row.getAs[String]("sid"), row)
    }.groupByKey()

    println("===================data_2========================")
    data_2.foreach(x=>println(x))

    val data_3: RDD[Row] = data_2.flatMap{ row =>{
      val ret_arr = new ArrayBuffer[Row]()
      val sid  = row._1
      val row_iter = row._2.iterator
      val version_arr: Array[String] = row_iter.next().getAs[String]("appversion").split('.')
      var flag:Boolean = false
      val json = new JsonParser()
      if(version_arr(1)>="5"&& version_arr(2)>="4"){flag = true}
        for(x: Row <- row_iter if flag){

          val obj: JsonObject = json.parse(x.getAs[String]("event_json")).asInstanceOf[JsonObject]
          val prop = obj.get("properties").getAsJsonObject
          if(prop.has("course_type")){
            val course_type: String = prop.get("course_type").getAsString
            ret_arr.+=(Row(sid,course_type,1.0))
          }
        }
      ret_arr
    }
    }

    val freq_coursetype: RDD[Row] = data_3.map{
      row=>{(Tuple2(row.getAs[String](0),row.getAs[String](1)),row.getAs[Double](2))}
    }.reduceByKey(_+_)
      .map{
        row=>(row._1._1,Tuple2(row._1._2,row._2))
      }.groupByKey
      .map{
        group_data => {
          val freq_coursetype_iter = group_data._2.toIterator.toList.sortBy(x=>x._2)(Ordering.Double.reverse).toIterator
          var count = 0
          val top3_arr = Array("","","")

          while(count<3 && freq_coursetype_iter.hasNext){
            top3_arr.update(count,freq_coursetype_iter.next()._1)
            count+=1
          }
          val type_seq:Map[String,String]  = Map("1" -> top3_arr(0), "2" -> top3_arr(1), "3" -> top3_arr(2))
          val type_seq_json = JSONObject(type_seq).toString()
          Row(group_data._1,"班型偏好",type_seq_json)
        }
      }
    freq_coursetype.foreach(x => println(x))
    freq_coursetype
  }
*/

  /**
    *
    * @param context
    * @param date_stamp 时间戳
    * @return
    */

  def cal_order_record(context: HiveContext, date_val:String, date_stamp:String)={
    import context.implicits._
    val data: DataFrame = context.sql("select * from ods.xes_stu_user_order " +
      "where finacreatedate>'2016-09-01' and sid is not null and sid !='' and sid !='-'" +
      " and dt "+ date_val +" limit 1000").toDF()

    val data_2: RDD[(String, Iterable[Row])] = data.map{
      row => Tuple2(row.getAs("sid").toString,row)
    }.groupByKey

    println("==================order_data_2======================")

    val data_3: RDD[Row] = data_2.flatMap{
      row =>{
        val ret_rdd_arr =  new ArrayBuffer[Row]()
        val iter: Iterator[Row] = row._2.iterator

        for(x<-iter){
          val id:String = x.getAs[String]("sid")
          val price_weight:Double = cal_price(x.getAs[Byte]("orderstatus"),
            x.getAs[Double]("classprice"))
          val level = match_level(x.getAs("classlevelname"))
          val subject = match_subject(x.getAs("subjectname"))
          val order_Weight = order_status2weight(x.getAs[Byte]("orderstatus"))
          ret_rdd_arr.+=(Row(id,subject+level,order_Weight)) //科目等级偏好
          ret_rdd_arr.+=(Row(id,subject,2.0)) //科目偏好
          ret_rdd_arr.+=(Row(id,subject+"price",price_weight)) //科目消费
        }
        ret_rdd_arr
      }
    }

    println("==================order_data_3======================")

    val ret_rdd: RDD[Row] = data_3.map{
        row => (Tuple2(row.getAs[String](0),row.getAs[String](1)),row.getAs[Double](2))
      }.reduceByKey(_+_)
        .map{
          row=>{
            Row(row._1._1,row._1._2,row._2,date_stamp)
          }
        }
    ret_rdd.foreach(x => println(x))
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
    val event_data: DataFrame = context.sql("select * from ods.user_behavior_event " +
      "where sid is not null and sid != '-' and sid !='' " +
      "and cla_id is not null and cla_id !='-' and cla_id!='' " +
      "and day  "+ date_val+" limit 1000").toDF()
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
          Row(row._1._1,row._1._2,row._2,date_stamp)
        }
      }
    println("=======================Plus_data======================")
    println(Plus_data.count())
    Plus_data.foreach(x=>println(x))
    Plus_data
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test_1000")
    val spark = new SparkContext(conf)
    val context = new HiveContext(spark)

    val schemaString = "sid,feature_name,value,dt"
    val fields=schemaString.split(",").map(fieldName => StructField(fieldName, StringType, nullable = true))
    fields.update(2,StructField("value", DoubleType, nullable = true))
    val schema=StructType(fields)

    println("==================goto event_calculate=======================")
    val event_rdd: RDD[Row] = cal_event_record(context,"='2018-08-20'","2018-09-05")
    val result1 = context.createDataFrame(event_rdd,schema)
    result1.foreach(x=>println(x))
//    result1.write.mode(SaveMode.Append).saveAsTable("rtestdb.eventcontent_local_test")


    println("==================goto order_calculate=======================")
    val order_rdd: RDD[Row] = cal_order_record(context,date_val="='2018-08-17'",date_stamp = "2018-09-05")
    val result2 = context.createDataFrame(order_rdd,schema)
    result2.foreach(x=>println(x))
//    result2.write.mode(SaveMode.Append).saveAsTable("rtestdb.ordercontent_local_test")



    println("==================goto event_order_calculate=======================")

    val datestamp = "2018-09-05"
    val event_order_rdd = event_rdd.union(order_rdd)
    val ret_rdd: RDD[Row] = event_order_rdd.map(row=>{
      (Tuple2(row.getAs[String](0),row.getAs[String](1)),row.getAs[Double](2))
    }).reduceByKey(_+_)
      .map(row=>Row(row._1._1,row._1._2,row._2,datestamp))

    ret_rdd.foreach(x=>println(x))

//    val order_event_content_result = context.createDataFrame(ret_rdd,schema)
//    order_event_content_result.write.mode(SaveMode.Append).saveAsTable("rtestdb.ordereventcontent_local_test")

//    println("==================goto freq_calculate=======================")
//    val freq_coursetype = cal_list_output(context)
//    val freq_field=schemaString.split(",").map(fieldName => StructField(fieldName, StringType, nullable = true))
//    val freq_schema=StructType(freq_field)
//    val freq_result = context.createDataFrame(freq_coursetype,freq_schema)
//    freq_result.write.mode(SaveMode.Append).saveAsTable("rtestdb.freqcontentrecall_yarn_test")

    spark.stop()

  }

}
