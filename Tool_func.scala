package content_recall

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex
import scala.util.parsing.json.JSONObject

object Tool_func {

  /**
    *
    * @param Rdd1 需要融合的RDD
    * @param Rdd2 需要融合的RDD
    * @param DateStamp 时间戳
    * @return Ret_Rdd
    */

  def MergeData(Rdd1:RDD[Row],Rdd2:RDD[Row],DateStamp:String)={
    val Sum_Rdd = Rdd1.union(Rdd2)
    val Ret_rdd: RDD[Row] = Sum_Rdd.map{
      row=>{
        (Tuple2(row.getAs[String](0),row.getAs[String](1)),row.getAs[Double](2))
      }
    }.reduceByKey(_+_)
      .map(row=>Row(row._1._1,row._1._2,row._2,DateStamp))
    Ret_rdd
  }


  /**
    * 以array形式输出用户最近三次购买的（1,2,3,4,5）课程的模式，匹配关联规则
    * @param context:HiveContext
    * @param date_val:String
    * @param output_name:String
    * @param dt:String 时间戳 当天日期
    */
  def CalRuleFunc(context: HiveContext,date_val: String,output_name:String,dt:String) = {

    val today = GetToday()
    context.sql("CREATE TABLE IF NOT EXISTS tss.rule_test(sid String,city_id String,grade_id String,left_rule ARRAY<String>,dt String)")
    val flag = context.sql("select * from tss.rule_test limit 1").take(1)(0).getAs[String]("dt").equals(today)
    if(!flag){
      val currUser = context.sql("select id from ods.xes_ods_uc_users")
      val currStudent = context.sql("select " +
        "stu_id," +
        "stu_uid " +
        "from ods.xes_ods_bn_tb_student")
      val currSid = currStudent.join(currUser,currStudent("stu_uid")===currUser("id"),"inner")
        .select("stu_id")

      val regData = context.sql("select reg_student_id," +
        "reg_class_id," +
        "reg_pay_date " +
        "from ods.xes_ods_bn_tb_regist " +
        "where reg_ispay in (1,2) and reg_isdeleted=0 and reg_pay_date "+ date_val +
        " and reg_student_id!='' and reg_student_id is not null and reg_student_id!='-'" +
        " and reg_class_id!='' and reg_class_id is not null and reg_class_id!='-'")

      val currSidReg = currSid.join(regData,currSid("stu_id")===regData("reg_student_id"),"inner")
        .select("stu_id",
          "reg_class_id",
          "reg_pay_date")

      val allClass = context.sql("select " +
        "cla_id," +
        "cla_term_id," +
        "cla_subject_names " +
        "from ods.xes_ods_bn_tb_class " +
        "where cla_year>'2016'" +
        " and cla_id is not null and cla_id !='' and cla_id !='-'" +
        " and cla_term_id in ('1','2','3','4','5')" +
        " and cla_subject_names in ('语文','数学','英语','物理','化学','综合','生物')")

      val currSidCla = currSidReg.join(allClass,currSidReg("reg_class_id")===allClass("cla_id"),"inner")
        .select("stu_id",
          "cla_id",
          "cla_term_id",
          "cla_subject_names",
          "reg_pay_date")

      val OutputData: RDD[Row] = currSidCla.map {
        row => (row.getAs[String]("stu_id"), row)
      }.groupByKey
        .map{
          a_group=>{
            val sid = a_group._1
            val IterByFintime = a_group._2.toIterator.toList.sortBy(x=>x.getAs[String]("reg_pay_date"))(Ordering.String.reverse).toIterator
            var count = 0
            val RuleList = Array("","","")

            while(IterByFintime.hasNext && count<3){
              val Record = IterByFintime.next()
              val Term = Record.getAs[String]("cla_term_id")
              val Subject = match_subject_forRule(Record.getAs[String]("cla_subject_names"))
              if(!RuleList.contains(Subject+"_"+Term)){
                RuleList.update(count,Subject+"_"+Term)
                count += 1
              }
            }
            Row(sid,output_name,RuleList,dt)
          }
        }

      val ruleSchema = StructType(
        Array(
          StructField("sid",StringType,true),
          StructField("feature_name",StringType,true),
          StructField("value",ArrayType.apply(StringType),true),
          StructField("dt",StringType,true)
        )
      )
      val RuleDf = context.createDataFrame(OutputData,ruleSchema)

      val StuCityGrade = context.sql("select " +
        "stu_id," +
        "city_id," +
        "stu_grade_id " +
        "from ods.xes_ods_bn_tb_student " +
        "where city_id is not null and city_id != '' and city_id!='' " +
        "and stu_grade_id is not null and stu_grade_id !='-' and stu_grade_id!='' ")

      val JoinDF = RuleDf.join(StuCityGrade,RuleDf("sid") === StuCityGrade("stu_id"),"inner")
      val RuleOutputDF = JoinDF.select("sid",
        "city_id",
        "stu_grade_id",
        "value",
        "dt")
        .toDF("sid",
          "city_id",
          "grade_id",
          "left_rule",
          "dt")

      RuleOutputDF.write.mode(SaveMode.Overwrite).saveAsTable("tss.rule_test")
      RuleOutputDF

    }
    else{
      val ReadFromDB = context.sql("select * from tss.rule_test")
      ReadFromDB
    }

  }


  /**
    *
    * @param context:HiveContext
    * @param date_val:String 时间范围 e.g. ("='2018-09-01'")
    * @param feature_name:String 需要计算的指标名称 String
    * @return 输出格式为RDD[ROW(sid:String,featureVal:String,Weight:Double...)]
    *         e.g.统计常选择的教师时 RDD[ROW(学生id,教师id,出现次数)]
    */
  def CalListValWeight(context: HiveContext,date_val:String,
                        feature_name: String,Condition:String="") = {
    import context.implicits._
    val RawData = context.sql("select sid,"+feature_name+" from ods.user_behavior" +
      " where sid!='' and sid!='-' and sid is not null" +
      " and day "+ date_val +
      "and " +feature_name+ " is not null and " +feature_name+ " !='' and " +feature_name+ " !='-' "+
      Condition).toDF

    val GroupedData: RDD[(String, Iterable[Row])] = RawData.map {
      row =>(row.getAs[String]("sid"), row)
    }.groupByKey

    val FeatureValCount: RDD[Row] = GroupedData.flatMap{
      row =>{
        val ret_arr = new ArrayBuffer[Row]()
        val sid  = row._1
        val row_iter = row._2.iterator

        for(x: Row <- row_iter ){
          val featureVal: String = x.getAs[String](feature_name)
          ret_arr.+=(Row(sid,featureVal,1.0))
        }
        ret_arr
      }
    }
    val OutputData: RDD[Row] = FeatureValCount.map{
      row=>{(Tuple2(row.getAs[String](0),row.getAs[String](1)),row.getAs[Double](2))}
    }.reduceByKey(_+_).map{
      line => Row(line._1._1,line._1._2,line._2)
    }
    OutputData
  }

  /**
    *
    * @param IntputRdd 输入的RDD 格式为sid:String,指标名:String,权重值:Double...
    * @param OutputName 输出的指标名称 （feature_name）
    * @param DateStamp 时间戳 : String
    * @return
    */

  def SortListOutput(IntputRdd:RDD[Row],OutputName:String,DateStamp:String)={
    val OutputRDD = IntputRdd.map{
      row=>(row.getAs[String](0),Tuple2(row.getAs[String](1),row.getAs[Double](2)))
    }.groupByKey
      .map{
        group_data => {
          val iter = group_data._2.toIterator.toList.sortBy(x=>x._2)(Ordering.Double.reverse).toIterator
          var count = 0
          val top3_arr = Array("","","")

          while(count<3 && iter.hasNext){
            top3_arr.update(count,iter.next()._1)
            count+=1
          }
          val RetList:Map[String,String]  = Map("1" -> top3_arr(0), "2" -> top3_arr(1), "3" -> top3_arr(2))
          val RetListJson = JSONObject(RetList).toString()
          Row(group_data._1,OutputName,RetListJson,DateStamp)
        }
      }
    OutputRDD
  }


  def transform_curriculum(context: HiveContext,city_code:String) = {
    val today = GetToday()

    val ClassCuc = context.sql("select cuc_class_id," +
      "cuc_start_time," +
      "cuc_end_time," +
      "cuc_class_date " +
      "from ods.xes_ods_bn_tb_curriculum " +
      "where cuc_class_date >'"+ today +"' and city_id = '"+ city_code +"'")

    val SeqlizeCucRdd = ClassCuc.map{
      row => (row.getAs[String]("cuc_class_id"),row)
    }.groupByKey
      .map{
        agroup => {
          val Cla_id = agroup._1
          val giter = agroup._2.toIterator
          val DateArr = new ArrayBuffer[String]()
          val StartTimeArr = new ArrayBuffer[String]()
          val EndTimeArr = new ArrayBuffer[String]()
          while(giter.hasNext){
            val aRecord = giter.next
            DateArr.+=(aRecord.getAs[java.util.Date]("cuc_class_date").toString)
            StartTimeArr.+=(aRecord.getAs[String]("cuc_start_time"))
            EndTimeArr.+=(aRecord.getAs[String]("cuc_end_time"))
          }
          Row(Cla_id,city_code,DateArr.toArray,StartTimeArr.toArray,EndTimeArr.toArray,today)
        }
      }

    val Schema = StructType(
      Array(
        StructField("cuc_id",StringType,true),
        StructField("city_id",StringType,true),
        StructField("date_arr", ArrayType.apply(StringType),true),
        StructField("starttime_arr", ArrayType.apply(StringType),true),
        StructField("endtime_arr", ArrayType.apply(StringType),true),
        StructField("dt",StringType,true)
      )
    )


    val WriteDf = context.createDataFrame(SeqlizeCucRdd,Schema)
    WriteDf.write.mode(SaveMode.Append).saveAsTable("tss.curriculum_transform")
    WriteDf
  }



  /**
    * 据正则判断班级等级，文字转数字
    * @param classLevelname
    * @return 匹配班等级 启航:1，勤思:2，敏学:3，
    *         博学:4,创新:5,兴趣:6:level_Other:0
    */
  def match_level(classLevelname:String):String={
    val Patter_l1: Regex = "(.*启航.*)".r
    val Patter_l2: Regex = "(.*勤思.*)".r
    val Patter_l2_2: Regex = "(.*勤学.*)".r
    val Patter_l3: Regex = "(.*敏学.*)".r
    val Patter_l4: Regex = "(.*博学.*)".r
    val Patter_l5: Regex = "(.*创新.*)".r
    val Patter_l6: Regex = "(.*兴趣.*)".r
    classLevelname match{
      case Patter_l1(`classLevelname`) => "_lev_1"
      case Patter_l2(`classLevelname`) => "_lev_2"
      case Patter_l2_2(`classLevelname`) => "_lev_2"
      case Patter_l3(`classLevelname`) => "_lev_3"
      case Patter_l4(`classLevelname`) => "_lev_4"
      case Patter_l5(`classLevelname`) => "_lev_5"
      case Patter_l6(`classLevelname`) => "_lev_6"
      case _ => "0"
    }
  }


  /**
    * 将非统计科目归纳为其他
    * @param subjectName
    * @return 将除语数英物化综合外的科目归为其他
    *         语文:chn\数学:math\英语:eng\物理:phy\化学:chem\综合:comp\其他:other
    */
  def match_subject(subjectName:String):String = {
    val Chn_reg: Regex = "(.*语文.*)".r
    val Math_reg: Regex = "(.*数学.*)".r
    val Eng_reg: Regex = "(.*英语.*)".r
    val Phy_reg: Regex = "(.*物理.*)".r
    val Chem_reg: Regex = "(.*化学.*)".r
    val Comp_reg: Regex = "(.*综合.*)".r
    subjectName match{
      case Chn_reg(subjectName) => "chn"
      case Math_reg(subjectName) => "math"
      case Eng_reg(subjectName) => "eng"
      case Phy_reg(subjectName) => "phy"
      case Chem_reg(subjectName) => "chem"
      case Comp_reg(subjectName) => "comp"
      case _ => "other"
    }
  }

  def match_subject_forRule(subjectName:String):String = {
    val Chn_reg: Regex = "(.*语文.*)".r
    val Math_reg: Regex = "(.*数学.*)".r
    val Eng_reg: Regex = "(.*英语.*)".r
    val Phy_reg: Regex = "(.*物理.*)".r
    val Chem_reg: Regex = "(.*化学.*)".r
    val Comp_reg: Regex = "(.*综合.*)".r
    subjectName match{
      case Chn_reg(subjectName) => "语文"
      case Math_reg(subjectName) => "数学"
      case Eng_reg(subjectName) => "英语"
      case Phy_reg(subjectName) => "物理"
      case Chem_reg(subjectName) => "化学"
      case Comp_reg(subjectName) => "综合"
      case _ => "其他"
    }
  }


  /**
    * 判断订单状态，赋予不同权重。
    * @param orderStuts
    * @return 0:购课单，1:提交待付款，2:已付款，3:退款
    *         据订单状态返回对应的权重
    *         购课单: 3, 提交待付款:5, 已付款:7, 退款:0.01, NULL:1
    */

  def order_status2weight(orderStuts:Byte):Double={
    orderStuts match{
      case 0 => 3
      case 1 => 5
      case 2 => 7
      case 3 => -2
      case _ => 1
    }
  }


  /**
    * 清洗课程类型
    * @param order_stuts  订单状态
    * @param price 订单价格
    * @return 0:购课单，1:提交待付款，2:已付款，3:退款
    */

  def cal_price(order_stuts:Byte,price:Double):Double={
    order_stuts match {
      case 0 => if(price<1000) price else price/10
      case 1 => if(price<1000) price else price/5
      case 2 => if(price<1000) price else price/2
      case 3 => price/1000
      case _ => 0

    }
  }


  /**
    * 清洗课程类型
    * @param classtype
    * @return 对课程类型做清洗，空值和“8”类型订单返回0,1:在线,2:双师,3:面授。
    */

  def wash_cla_type(classtype:String):String={
    if(classtype == "1" || classtype == "2" ||classtype =="3")
      "_tp_"+classtype.toString
    else
      "_tp_0"
  }


  /**
    * 时间戳转日期。
    * @param tm
    * @return tim 相应格式日期
    */

  def tranTimeToString(tm:String) :String={
    val fm = new SimpleDateFormat("yyyy-MM-dd")
    val tim = fm.format(new Date(tm.toLong))
    tim
  }


  def TimeCover(fs:String,fe:String,rs:String,re:String):Boolean = {
    if(rs>fe || fs>re) false else true
  }

  def GetToday(): String ={
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val today: String = dateFormat.format(now)
    today
  }

}
