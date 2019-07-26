package ccic.net.bi.tools
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.util.Properties
import java.text.SimpleDateFormat
import org.apache.spark.sql.SparkSession
import ccic.net.bi.udf.define_ccic_udf


object SparkTrans {
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  val LOGGER = Logger.getLogger(SparkTrans.getClass)
  //获取当前sparksession
  val spark= SparkSession.builder
                         .appName("s ${this.getClass.getSimpleName}")
                         .enableHiveSupport()
                         .getOrCreate()
  def process_overwrite(saveMode: String,sql: String, outputName: String){
    
  }
  
  def process_append(saveMode: String,sql: String, outputName: String){
    
  }
  
  def insert_partition(saveMode: String,sql: String, outputName: String){
    
  }
}