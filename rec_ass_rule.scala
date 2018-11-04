package main.content_recall

import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import com.alibaba.fastjson.{JSON, JSONArray}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer
import content_recall.Tool_func.{TimeCover, transform_curriculum}

import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

object rec_ass_rule {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("yarn-cluster").setAppName("Mym")
    val spark = new SparkContext(conf)
    val context = new HiveContext(spark)

    import context.implicits._
    //以广州市为例
    val RuleLeft = context.sql("select * from tss.rule_test where city_id = '020'")
    val RuleRight = context.sql("select * from tss.recom_recall where recall != '{}' and city_id = '020'")
    val Location = context.sql("select sid as ssid,depart_4 from tss.loc_user_department where city_id = '020'")
    //sid as ssid 连接后的df存在两个sid 无法索引 故用别名区别，spark升级后可通过df1.join(df2,seq("key1","key2"),"inner")解决

    val RuleUnion = RuleLeft.join(RuleRight,RuleLeft("city_id") === RuleRight("city_id")&&RuleLeft("grade_id") === RuleRight("grade_id"),
    "inner").select("sid","left_rule","left","recall")
    val RuleLocUnion = RuleUnion.join(Location,RuleUnion("sid") === Location("ssid"),"inner")
      .select("sid","left_rule","left","recall","depart_4")

    val GroupedData = RuleLocUnion.map(row => (row.getAs[String]("sid"),row)).groupByKey
    val recResult: RDD[Row] = GroupedData.map{
      agroup => {
        val sid = agroup._1
        val gIter = agroup._2.toIterator
        val recClassList = new ArrayBuffer[String]()
        val liveKey = "live-class-service-center"
        while(gIter.hasNext){
          val aRule:Row = gIter.next()
          val stuLeft = aRule.getAs[Seq[String]]("left_rule")
          val ruleLeft = aRule.getAs[Seq[String]]("left")
          var flag = true
          for(x <- ruleLeft if flag){ // 判断匹配关联规则
            if(!stuLeft.contains(x)) flag=false
          }
          val claList = aRule.getAs[String]("recall")
          val clajsonObj = JSON.parseObject(claList)
          if(flag) {
            val locList: Array[String] = aRule.getAs[String]("depart_4").split(',')
            for(loc <- locList){
              if(clajsonObj.containsKey(loc)){
                val claListStr: JSONArray = clajsonObj.getJSONArray(loc)
                recClassList.++=(claListStr.toArray(new Array[String](claListStr.size())))
              }
            }
            if(clajsonObj.containsKey(liveKey)){
              val liveClass  = clajsonObj.getJSONArray(liveKey)
              recClassList.++=(liveClass.toArray(new Array[String](liveClass.size())))
            }
          }
        }
        val recClassSet: Array[String] = recClassList.toSet.toArray//去重
        Row(sid,"020",recClassSet)
      }
    }

    //去空推荐结果 写入表结构tss.loc_rule_rec
    val recSchema = StructType(
      Array(
        StructField("sid",StringType,true),
        StructField("city_id",StringType,true),
        StructField("rec_class_list",ArrayType.apply(StringType),true)
      )
    )


    val recDf = context.createDataFrame(recResult,recSchema)
    val filterNullDf: DataFrame = recDf.filter(!recDf("rec_class_list")(0).equalTo(""))
    filterNullDf.write.mode(SaveMode.Append).saveAsTable("tss.loc_rule_rec")

    //读入推荐结果,转纵表方便拼接cla_current表
    val readData = context.sql("select * from tss.loc_rule_rec where city_id ='020'")
    val FlatData: RDD[Row] = readData.flatMap{
      row => {
        val sid = row.getAs[String]("sid")
        val city_id = row.getAs[String]("city_id")
        val recClasses  = row.getAs[Seq[String]]("rec_class_list")
        val recordArr = new ArrayBuffer[Row]()
        for(cla_id <- recClasses){
            recordArr.+=(Row(sid,city_id,"-1",cla_id)) //"-1"为reg_ispay字段
          //reg_ispay 为-1 与sid_register union 时用reg_ispay 作区分
          }
        recordArr
      }
    }

    val flatSchema = StructType(
      Array(
        StructField("sid",StringType,true),
        StructField("city_id",StringType,true),
        StructField("reg_ispay",StringType,true),
        StructField("cla_id",StringType,true)
      )
    )

    val FlatDf = context.createDataFrame(FlatData,flatSchema)

    val allClass = context.sql("select cla_id as class_id," +
      "cla_term_id," +
      "cla_subject_names," +
      "cla_year " +
      "from tss.cla_current " +
      "where cla_id is not null and cla_id !='' and cla_id != '-' and city_id = '020' ")
    //拼接cla_current表
    val JoinedDf = FlatDf.join(allClass,FlatDf("cla_id") === allClass("class_id"),"inner")
    .select("sid",
      "cla_id",
      "cla_term_id",
      "reg_ispay",
      "cla_subject_names",
      "cla_year")

    //课程排期表转换
    val Curriculum = transform_curriculum(context,"020")

    //拼接课排期表
    val RecWithCurr = JoinedDf.join(Curriculum,JoinedDf("cla_id") === Curriculum("cuc_id"),"inner")
      .select("sid",
        "cla_id",
        "cla_term_id",
        "cla_subject_names",
        "cla_year",
        "reg_ispay",
        "date_arr",
        "starttime_arr",
        "endtime_arr")


    //RecWithCurr与SidRegister格式统一，方便union
    val ModifyRec = RecWithCurr.map{
      row => {
        val sid = row.getAs[String]("sid")
        val cla_id = row.getAs[String]("cla_id")
        val cla_term_id = row.getAs[String]("cla_term_id")
        val cla_subject_names = row.getAs[String]("cla_subject_names")
        val cla_year = row.getAs[String]("cla_year")
        val reg_ispay = row.getAs[String]("reg_ispay")
        val DayArr: Array[String] = row.getAs[Seq[String]]("date_arr").toArray
        val StartArr = row.getAs[Seq[String]]("starttime_arr").toArray
        val EndArr = row.getAs[Seq[String]]("endtime_arr").toArray
        Row(sid,cla_id,cla_term_id,cla_subject_names,cla_year,reg_ispay,DayArr,StartArr,EndArr)
      }
    }


    //读取sid_register表 以reg_isdeleted 和 city_id过滤
    val SidRegister = context.sql("Select " +
      "sid," +
      "cla_id," +
      "cla_term_id," +
      "cla_subject_names," +
      "year," +
      "reg_ispay," +
      "class_dates," +
      "start_times," +
      "end_times " +
      "from tss.sid_register where reg_isdeleted = '0' and city_id = '020'")


    val TransformReg = SidRegister.map{
      row=>{
        val sid = row.getAs[String]("sid")
        val cla_id = row.getAs[String]("cla_id")
        val cla_term_id = row.getAs[String]("cla_term_id")
        val cla_subject_names = row.getAs[String]("cla_subject_names")
        val cla_year = row.getAs[String]("year")
        val reg_ispay = row.getAs[String]("reg_ispay")
        val DayArr = row.getAs[String]("class_dates").split("\001")
        val StartArr = row.getAs[String]("start_times").split("\002")
        val EndArr = row.getAs[String]("end_times").split("\003")

        Row(sid,cla_id,cla_term_id,cla_subject_names,cla_year,reg_ispay,DayArr,StartArr,EndArr)
      }
    }


    val unionSchema = StructType(
      Array(
        StructField("sid",StringType,true),
        StructField("cla_id",StringType,true),
        StructField("cla_term_id",StringType,true),
        StructField("cla_subject_names",StringType,true),
        StructField("year",StringType,true),
        StructField("reg_ispay",StringType,true),
        StructField("class_dates",ArrayType.apply(StringType),true),
        StructField("start_times",ArrayType.apply(StringType),true),
        StructField("end_times",ArrayType.apply(StringType),true)
      )
    )

    val RuleDf = context.createDataFrame(TransformReg,unionSchema)

    val RecDf = context.createDataFrame(ModifyRec,unionSchema)


    RecDf.show(1)
    //RuleDf 与 RecDf做union
    val UnionRuleRec = RuleDf.unionAll(RecDf)
    UnionRuleRec.show(2)
    val unionGroup = UnionRuleRec.map{
      row => (row.getAs[String]("sid"),row)
    }.groupByKey

    //推荐结果过滤
    val AfterFilterRec: RDD[Row] = unionGroup.map{
      agroup =>{
        val sid = agroup._1
        val recList = new ArrayBuffer[Row]()
        val filterList = new ArrayBuffer[Row]()
        val retList = new ArrayBuffer[String]()
        val recordIter = agroup._2.toIterator
        while(recordIter.hasNext){
          val r: Row = recordIter.next()
          if(r.getAs[String]("reg_ispay").equals("-1")) recList.+=(r.copy()) else filterList.+=(r.copy())
        }

        for(arec<-recList){
          var flag = true  //是否添加到最终结果中。
          val rec_termsubyear: String = arec.getAs[String]("cla_term_id")+
            arec.getAs[String]("cla_subject_names")+
            arec.getAs[String]("year")
          for(fil<-filterList if flag){
            val fil_termsubyear: String = fil.getAs[String]("cla_term_id")+
              fil.getAs[String]("cla_subject_names")+
              fil.getAs[String]("year")
            //判断学期-科目-年份是否一致，若不一致则判断上课日期是否冲突
            if(rec_termsubyear.equals(fil_termsubyear)){
              flag = false
            }
            else {
              val recdays = arec.getAs[Seq[String]]("class_dates")
              val recstarts = arec.getAs[Seq[String]]("start_times")
              val recends = arec.getAs[Seq[String]]("end_times")

              val fildays = fil.getAs[Seq[String]]("class_dates")
              val filstarts = fil.getAs[Seq[String]]("start_times")
              val filends = fil.getAs[Seq[String]]("end_times")

              for(aday <- recdays){
                if(fildays.contains(aday)){
                  if(TimeCover(filstarts(fildays.indexOf(aday)),
                    filends(fildays.indexOf(aday)),
                    recstarts(recdays.indexOf(aday)),
                    recends(recdays.indexOf(aday)))) flag=false
                }
              }
            }
          }
          if(flag) retList.+=(arec.getAs("cla_id"))

        }


        Row(sid,retList.toArray)
      }
    }

    val resultSchema = StructType(
      Array(
        StructField("sid",StringType,true),
        StructField("rec_list",ArrayType.apply(StringType),true)
      )
    )
    val AfterFilterRecDf = context.createDataFrame(AfterFilterRec,resultSchema)

    val RecFilternull = AfterFilterRecDf.filter(!AfterFilterRecDf("rec_list")(0).equalTo(""))

    RecFilternull.write.mode(SaveMode.Append).saveAsTable("tss.after_filter_recommand")

    //===========================================================================
    //===================================9-18====================================
    //===========================================================================

    val flatRec = RecFilternull.flatMap{
      row => {
        val ret = new ArrayBuffer[Row]()
        val sid = row.getAs[String]("sid")
        val recList= row.getAs[Seq[String]]("rec_list")
        for(rec_cla: String <- recList){
          ret.+=(Row(sid,rec_cla))
        }
        ret
      }
    }

    val flatRecSchema = StructType(
      Array(
        StructField("sid",StringType,true),
        StructField("rec_class_id",StringType,true)
      )
    )
    val flatRecDf = context.createDataFrame(flatRec,flatRecSchema)

    val cla_current = context.sql("select * from tss.cla_current where city_id ='020'")

    val flatRecDf2 = flatRecDf.join(cla_current,flatRecDf("rec_class_id")===cla_current("cla_id"),"inner")
      .select("rec_class_id",
        "cla_class_type",
        "city_id",
        "cla_subject_ids",
        "cla_class_type",
        "cla_subject_names",
        "cla_term_id",
        "cla_grade_id",
        "cla_level_name",
        "cla_classtime_names",
        "cla_classdate_name")

    val claScore = context.sql("select cla_id,fin_score from cla_mjq_hot_score where city_id ='020'")

    val flatRecDf3 = flatRecDf2.join(claScore,flatRecDf2("rec_class_id")===claScore("cla_id"),"inner")
      .drop("rec_class_id")
      .map(row => (row.getAs[String]("sid"),row)).groupByKey

    val hitRdd = flatRecDf3.map{
        agroup =>{
          var pack_on =  Map[String,Map[String,String]]()
          var pack_off =  Map[String,Map[String,String]]()
          val sid = agroup._1
          val giter = agroup._2.toIterator
          while(giter.hasNext){
            var info = Map[String,String]()
            val arow = giter.next()
            val cla_id = arow.getAs[String]("cla_id")
            info.+=("cla_id"->arow.getAs[String]("cla_id"))
            info.+=("city_id"->arow.getAs[String]("city_id"))
            info.+=("cla_subject_ids"->arow.getAs[String]("cla_subject_ids"))
            info.+=("cla_class_type"->arow.getAs[String]("cla_class_type"))
            info.+=("cla_subject_names"->arow.getAs[String]("cla_subject_names"))
            info.+=("cla_term_id"->arow.getAs[String]("cla_term_id"))
            info.+=("cla_grade_id"->arow.getAs[String]("cla_grade_id"))
            info.+=("cla_level_name"->arow.getAs[String]("cla_level_name"))
            info.+=("cla_classtime_names"->arow.getAs[String]("cla_classtime_names"))
            info.+=("cla_classdate_name"->arow.getAs[String]("cla_classdate_name"))
            info.+=("getScore"->arow.getAs[String]("fin_score"))

            if(arow.getAs[Int]("cla_class_type")==1)
              pack_on.+=(cla_id->info)
            else
              pack_off.+=(cla_id->info)

          }
          var pack_on_off = Map[String,Map[String,Map[String,String]]]()
          pack_on_off.+=("online_recom"->pack_on)
          pack_on_off.+=("offine_recom"->pack_off)
          val packjson: String = compact(render(pack_on_off))
          Row(sid,packjson)
        }
      }

    val hitSchema = StructType(
      Array(
        StructField("sid",StringType,true),
        StructField("json",StringType,true)
      )
    )
    val hitDf = context.createDataFrame(hitRdd,hitSchema)

  }
}
