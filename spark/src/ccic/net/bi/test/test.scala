package ccic.net.bi.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import java.lang.Long

object startScala {
  
  def isupper():Boolean={
   val name="liaHpjie"
   val state=name.exists(_.isUpper);
   
    return state;
  }
  
def main(args: Array[String]): Unit ={
  println(this.isupper())
//  val conf = new SparkConf()
//    .setAppName("QJZK")
//    .setMaster("local")
//  //val sc = new SparkContext(conf)
//  //val sQLContext = new SQLContext(sc)
//  val spark=SparkSession.builder.config(conf).getOrCreate()
//  val filerdd=spark.sparkContext.textFile("D:/wenzhang.txt")
////  val word=filerdd.flatMap(line=>line.split(",")).map(x=>1).reduce((x,y)=>x+y)
////  println(word)
//  val word=filerdd.flatMap(a=>a.split(",")).filter(_.contains("a"))
//  .map(word=>(word,1)).reduceByKey((x,y)=>x+y)
//  //word.collect()
//  println(word.count())
//  word.collect().foreach(println)
////  word.take(5).foreach(println)
  
  
  
  
  
  
// val hadooprdd=spark.read.load("D:\\part-00299-dcef438e-ac1a-492f-8f18-a0df72edcc2d-c000.snappy.parquet")
//  val word=spark.sparkContext.textFile("D:/Hive2Mysql.java");
  //val num=word.count();
//  hadooprdd.printSchema();
//  hadooprdd.show(1);
//  hadooprdd.createTempView("deal_policy_summary_adj")
//  spark.sql("select policyno from deal_policy_summary_adj").show(1)
  
 
  
  //读取一个Parquet文件
  //val paquetDF = spark.read.load("D:\\work\\input\\*")
   // println("计数为"+num)
  //读取Parquet文件Schema结构
  //val parquetschema = sQLContext.parquetFile("D:\\work\\input\\*.parquet")

//  println(paquetDF.count())
//  paquetDF.show()
}
}