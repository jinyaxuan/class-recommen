package main.content_recall

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import content_recall.Tool_func.{CalListValWeight, MergeData, SortListOutput}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
object list_Output{

  /**
    *
    * @param context:HiveContext
    * @param beginDate:String 开始日期
    * @param endDate:String 截止日期，开始与截止日期构成闭区间
    * @param FeatureName:String 需要统计的字段名称,例如若统计教师偏好，则传入"cla_teacher_ids"
    * @param Condition:String 附加的数据筛选条件，会拼接到sql最后 e.g. " and sid = '123456'"
    * @return
    */
  def IterCal(context:HiveContext,beginDate:String,endDate:String,FeatureName:String,Condition:String="")={
    val Fm = new SimpleDateFormat("yyyy-MM-dd")
    var UnionData: RDD[Row] = CalListValWeight(context,"='"+ beginDate +"'",FeatureName,Condition)
    val StartDt = new Date(Fm.parse(beginDate).getTime + 24*3600*1000)
    val EndDt: Date = Fm.parse(endDate)
    while(StartDt.compareTo(EndDt)<1){
      val RddToPlus: RDD[Row] = CalListValWeight(context,"='"+Fm.format(StartDt)+"'",FeatureName,Condition)
      UnionData = MergeData(UnionData,RddToPlus,"2018-09-05")
      StartDt.setTime(StartDt.getTime + 24*3600*1000)
    }
    UnionData
  }

  def CalListOutputFunc(context:HiveContext,beginDate:String,endDate:String,FeatureName:String,
                        OutputName:String,DateStamp:String,Condition:String="")={
    val UnionData = IterCal(context,beginDate,endDate,FeatureName,Condition)
    val Result = SortListOutput(UnionData,OutputName,DateStamp)
    Result
  }



  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("yarn-cluster").setAppName("Mym")
    val spark = new SparkContext(conf)
    val context = new HiveContext(spark)

    //定义DataFrame的Schema
    val SchemaString="sid,feature_name,value,dt"
    val Fields=SchemaString.split(",").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val Schema=StructType(Fields)

    val Teacher = CalListOutputFunc(context,"2018-08-10","2018-09-05",
      "cla_teacher_ids","teacher_favor","2018-09-05")
    val TeacherResultDf = context.createDataFrame(Teacher,Schema)
    TeacherResultDf.write.mode(SaveMode.Append).saveAsTable("tss.listouput_yarn_test")

    val ClassLocation = CalListOutputFunc(context,"2018-08-10","2018-09-05",
      "cla_servicecenter_id","class_location_favor","2018-09-05")
    val ClassLocationResultDf = context.createDataFrame(ClassLocation,Schema)
    ClassLocationResultDf.write.mode(SaveMode.Append).saveAsTable("tss.listouput_yarn_test")

    val ClassTime = CalListOutputFunc(context,"2018-08-10","2018-09-05",
      "cla_classtime_ids","class_time_favor","2018-09-05",
      "and cla_class_type='1' or cla_class_type='3'")
    val ClassTimeResultDf = context.createDataFrame(ClassTime,Schema)
    ClassTimeResultDf.write.mode(SaveMode.Append).saveAsTable("tss.listouput_yarn_test")

  }
}
