package content_recall

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.JedisPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.rdd.RDD
import com.alibaba.fastjson.JSON

object scala_Jedis{

  object getPool {
    // jedispool为null则初始化，
    val config = new GenericObjectPoolConfig
    // 控制一个pool可分配多少个jedis实例，通过pool.getResource()来获取；
    // 如果赋值为-1，则表示不限制；如果pool已经分配了maxTotal个jedis实例，则此时pool的状态为exhausted(耗尽）.
    config.setMaxTotal(16)
    config.setMaxIdle(8)// 控制一个pool最多有多少个状态为idle(空闲的)的jedis实例
    config.setMinIdle(0)

    def pool={
      new JedisPool(config, "172.31.0.118", 6379, 10000,"crs-gu9jkj28:7X@#fk6r@ai*A4fw" , 6)
    }//10000是protocol.timeout 默认值2000
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val spark = new SparkContext(conf)
    val context = new HiveContext(spark)
//    val data = context.sql("select * from ods.xes_ods_bn_tb_class limit 100")

//    val json_data: RDD[String] = data.toJSON
//
//    json_data.foreachPartition(x => {
//      x.foreach(y => println(y))
//    })
//    json_data.foreachPartition{
//      partition: Iterator[String] => {
//        import com.alibaba.fastjson.JSON
//        val pool_test = getPool.pool
//        val jedis_entity = pool_test.getResource
//        partition.foreach{
//          record => jedis_entity.setex(JSON.parseObject(record).getString("cla_id"),600,record)
//        }
//        jedis_entity.close()
//      }
//
//    }

    val jedis_entity1 = getPool.pool.getResource
    println("======================read_data=====================")
    println(jedis_entity1.get("recommend_020_7数学"))
    var i =0
    while(i<10){
      val k: String =jedis_entity1.keys("recommend_studentid_*").toArray()(i).toString
      println(jedis_entity1.get(k))
      i+=1
      println("count:"+i)
    }

    jedis_entity1.close()

    spark.stop()
  }



}
