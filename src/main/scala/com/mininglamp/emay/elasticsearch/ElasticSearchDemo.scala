package com.mininglamp.emay.elasticsearch

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.api.java.{JavaSparkContext, JavaRDD}
import org.apache.spark.rdd.RDD
import org.elasticsearch.spark.rdd.EsSpark

import org.json.JSONObject

object ElasticSearchDemo {

  def process(input: String): String ={
    val fields: Array[String] = input.split("\001".charAt(0))

    if (fields.length < 5) {
      return null
    }

    val msg_profile = new JSONObject()
    msg_profile.append("src", fields{0})
    msg_profile.append("tel", fields{1})
    msg_profile.append("submit_time", fields{2})
    msg_profile.append("smscontent", fields{3})
    msg_profile.append("id", fields{4})

    msg_profile.toString()
  }

  def msgcountprocess (tel: String, count: Integer): String = {
   val msg_count = new JSONObject()
    msg_count.put("tel", tel)
    msg_count.put("count", count.toString)
    msg_count.toString
  }


  def main (args: Array[String]): Unit = {

    try {
      // Basic command to check if can read and write to HDFS
      /*
      val sc = new SparkContext(new SparkConf())
      val textFile = sc.textFile("hdfs:///user/hbase/messageRule.txt")
      val counts = textFile.flatMap(line => line.split(" "))
        .map(word => (word, 1))
        .reduceByKey(_ + _)
      counts.saveAsTextFile("hdfs:///user/hbase/test.txt")
      */
      // test if file readable
      // /*
      val conf = new SparkConf().setAppName("Customer Profile Load").set("es.nodes", "localhost").set("es.port", "9200")
      val sc = new SparkContext(conf)
      val msgProfileInput = sc.textFile("hdfs:///user/hive/warehouse/emay_data.db/mt_info_day")
      val counts = msgProfileInput.map(line => line.split("\001".charAt(0))).filter(_.length >= 4)
        .map{ row => row(1)}.map{tel => (tel, 1)}
        .reduceByKey(_ + _)
      //counts.saveAsTextFile("hdfs:///user/hbase/test.txt") // 测试可读

      val countEs = counts.map{
        case (tel, count) =>
          msgcountprocess(tel, int2Integer(count))
      }
      //countEs.saveAsTextFile("""hdfs:///user/hbase/test.txt""")
      EsSpark.saveToEs(countEs, """msgcount/msgno""")
      // */

      /*
      val msgProfileInput = "hdfs:///user/hive/warehouse/emay_data.db/mt_info_day"
      val conf = new SparkConf().setAppName("Customer Profile Load").set("es.nodes", "localhost").set("es.port", "9200")
      conf.set("es.mapping.id", "header.msgId")
      val sc: SparkContext = new SparkContext(conf)
      val rawCustomerProfile = sc.textFile(msgProfileInput)
      val msgProfile = rawCustomerProfile.map{process}
      msgProfile.saveAsTextFile("""hdfs:///user/hbase/test.txt""")
      //EsSpark.saveToEs(msgProfile, "testmsgprofile/emay")
      */

      /*
      //import org.apache.spark.SparkContext
      import org.elasticsearch.spark.rdd.EsSpark

      // define a case class
      case class Trip(departure: String, arrival: String)

      val upcomingTrip = Trip("OTP", "SFO")
      val lastWeekTrip = Trip("MUC", "OTP")
      val sc = new SparkContext(new SparkConf())

      val rdd = sc.makeRDD(Seq(upcomingTrip, lastWeekTrip))
      EsSpark.saveToEs(rdd, "spark/docs")
      */

      // Demo 1
      /*
      val conf = new SparkConf().setAppName("Customer Profile Load").set("es.nodes", "localhost").set("es.port", "9200")
      val sc = new SparkContext(conf)
      val json1 = """{"reason" : "business", "airport" : "SFO"}"""
      val json2 = """{"participants" : 5, "airport" : "OTP"}"""

      val jsonO: Seq[String] = Seq(json1, json2)
      val rdd = sc.makeRDD(Seq(json1, json2))
      rdd.foreach(println)
      EsSpark.saveJsonToEs(rdd, "spark/json-trips")
      */
      // Demo 2 read
/*
      import org.elasticsearch.spark._
      val sc = new SparkContext(new SparkConf())
      val rdd = sc.esRDD("radio/artists", "?me*")
*/
      // Demo 3 read
/*
       import org.elasticsearch.spark._

      case class Artist(name: String, albums: Int)

      val u2 = Artist("U2", 12)
      val bh = Map("name" -> "Buckethead", "albums" -> 95, "age" -> 45)

      sc.makeRDD(Seq(u2, h2)).saveToEs("radio/artists")
*/


      //JavaPairRDD<String, ImmutableOpenMap<String, String>> tempPairRDD = new GenerateCustomerProfileESTuple()
      //        .process(msgProfile)
      //JavaEsSpark.saveToEsWithMeta(tempPairRDD, "testmsgprofile/unibet")
    } catch {

      // LOGGER.error("Encountered exception when processing the CustomerProfile [{}]", e.getMessage())
      case e: Exception => {
        System.err.println("Encountered exception when processing the CustomerProfile [{}]" + e.getMessage())
        throw e
      }
    }

  }
}