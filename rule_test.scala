//package main.content_recall
//
//import java.text.SimpleDateFormat
//import java.util.Date
//
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
//import content_recall.Tool_func.CalRuleFunc
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.{DataFrame, Row, SaveMode}
//
//object rule_test {
//
//  //ods.xes_ods_bn_tb_student
//
//  def RuleOutput(context:HiveContext): DataFrame ={
//    val now: Date = new Date()
//    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
//    val today: String = dateFormat.format(now)
//
//    val OutputData: RDD[Row] = CalRuleFunc(context,">'2016-09-01'", "rule_left",today)
//
//    val ruleSchema = StructType(
//      Array(
//        StructField("sid",StringType,true),
//        StructField("feature_name",StringType,true),
//        StructField("value",ArrayType.apply(StringType),true),
//        StructField("dt",StringType,true)
//      )
//    )
//
//    val RuleDf = context.createDataFrame(OutputData,ruleSchema)
//
//    val StuCityGrade = context.sql("select stu_id," +
//      "city_id," +
//      "stu_grade_id " +
//      "from ods.xes_ods_bn_tb_student " +
//      "where city_id is not null and city_id != '' and city_id!='' " +
//      "and stu_grade_id is not null and stu_grade_id !='-' and stu_grade_id!='' ")
//
//    val JoinDF = RuleDf.join(StuCityGrade,RuleDf("sid") === StuCityGrade("stu_id"),"inner")
//    val RuleOutputDF = JoinDF.select("sid",
//      "city_id",
//      "stu_grade_id",
//      "value",
//      "dt")
//      .toDF("sid",
//        "city_id",
//        "grade_id",
//        "left_rule",
//        "dt")
//
//    RuleOutputDF.write.mode(SaveMode.Overwrite).saveAsTable("tss.rule_test")
//    RuleOutputDF
//  }
//
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("yarn-cluster").setAppName("Mym")
//    val spark = new SparkContext(conf)
//    val context = new HiveContext(spark)
//
//
//    val OutputData: RDD[Row] = CalRuleFunc(context,">'2016-09-01'",
//      "rule_left","2018-09-08")
//
//    val ruleSchema = StructType(
//      Array(
//        StructField("sid",StringType,true),
//        StructField("feature_name",StringType,true),
//        StructField("value",ArrayType.apply(StringType),true),
//        StructField("dt",StringType,true)
//      )
//    )
//
//    val RuleDf = context.createDataFrame(OutputData,ruleSchema)
//
//    val StuCityGrade = context.sql("select stu_id,city_id,stu_grade_id from ods.xes_ods_bn_tb_student " +
//      "where city_id is not null and city_id != '' and city_id!='' " +
//      "and stu_grade_id is not null and stu_grade_id !='-' and stu_grade_id!='' ")
//
//    val JoinDF = RuleDf.join(StuCityGrade,RuleDf("sid") === StuCityGrade("stu_id"),"inner")
//    val RuleOutputDF = JoinDF.select("sid",
//      "city_id",
//      "stu_grade_id",
//      "value",
//      "dt")
//      .toDF("sid",
//        "city_id",
//        "grade_id",
//        "left_rule",
//        "dt")
//    RuleOutputDF.write.mode(SaveMode.Append).saveAsTable("tss.rule_test")
//  }
//
//}
