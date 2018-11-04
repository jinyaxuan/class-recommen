package main.content_recall

import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import redis.clients.jedis.JedisPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

object non_hit_process {

  object getPool {
    val config = new GenericObjectPoolConfig
    config.setMaxTotal(16)
    config.setMaxIdle(8)
    config.setMinIdle(0)

    def pool={
      new JedisPool(config, "172.31.0.118", 6379, 10000,"crs-gu9jkj28:7X@#fk6r@ai*A4fw" , 6)
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("yarn-cluster").setAppName("Mym")
    val spark = new SparkContext(conf)
    val context = new HiveContext(spark)

    val hotScore = context.sql("select " +
      "cla_id," +
      "city_id," +
      "cla_class_type," +
      "cla_subject_names," +
      "cla_subject_ids," +
      "cla_term_id, " +
      "cla_grade_id, " +
      "cla_level_name," +
      "cla_classtime_names," +
      "cla_classdate_name," +
      "fin_score " +
      "from tss.cla_mjq_hot_score")

    val nonHitRdd = hotScore.map{
      row => {
        (row.getAs[String]("city_id")+"_"+
          row.getAs[String]("cla_grade_id")+"_"+
          row.getAs[String]("cla_subject_names"),row)
      }
    }.groupByKey
      .map{
        agroup =>{
          val ret = new ArrayBuffer[Row]()
          val key = agroup._1
          val giter = agroup._2.toIterator
          var groupmap =  new ArrayBuffer[Map[String,String]]()
          while(giter.hasNext){
            val aclass = giter.next()
            var info = Map[String,String]()
            val cla_id = aclass.getAs[String]("cla_id")
            info.+=("cla_id"->aclass.getAs[String]("cla_id"))
            info.+=("city_id"->aclass.getAs[String]("city_id"))
            info.+=("cla_subject_ids"->aclass.getAs[Long]("cla_subject_ids").toString)
            info.+=("cla_class_type"->aclass.getAs[Long]("cla_class_type").toString)
            info.+=("cla_subject_names"->aclass.getAs[String]("cla_subject_names"))
            info.+=("cla_term_id"->aclass.getAs[String]("cla_term_id"))
            info.+=("cla_grade_id"->aclass.getAs[String]("cla_grade_id"))
            info.+=("cla_level_name"->aclass.getAs[String]("cla_level_name"))
            info.+=("cla_classtime_names"->aclass.getAs[String]("cla_classtime_names"))
            info.+=("cla_classdate_name"->aclass.getAs[String]("cla_classdate_name"))
            info.+=("getScore"->aclass.getAs[Float]("fin_score").toString)

            groupmap.+=(info)
          }
          val jstring: String = compact(render(groupmap))

          Row(key,jstring)
        }
      }

    val nonHitSchema = StructType(
      Array(
        StructField("key",StringType,true),
        StructField("jsonstring",StringType,true)
      )
    )

    val nonHitDf = context.createDataFrame(nonHitRdd,nonHitSchema)

    nonHitDf.foreachPartition{
      partition: Iterator[Row] => {

        val pool_test = getPool.pool
        val jedis_entity = pool_test.getResource
        partition.foreach{
          record => jedis_entity.setex("recommend_"+record.getAs[String]("key"),2592000,record.getAs[String]("jsonstring"))
        }
        jedis_entity.close()
      }
    }

  }

}
