package ccic.net.bi.tools
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.util.Properties
import java.text.SimpleDateFormat
import org.apache.spark.sql.SparkSession
import ccic.net.bi.udf.define_ccic_udf
import ccic.net.bi.tools.SparkTrans

object SparkModel {
  def main(args: Array[String]): Unit={
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  val LOGGER = Logger.getLogger(SparkModel.getClass)
  // 定义传入的变量
  val mode=args(0)
  val sql=args(1)
  val table_name=args(2)
  
  // 进入模式判断
  //总共三种模式 overwrite append partition
  mode match {
      //覆盖模式
      case "overwrite" => {
        print("开启数据覆盖插入模式")
        if (args.length != 3) {
          print("输入参数错误，请重新输入")
          LOGGER.error("输入参数错误，请重新输入")
          System.exit(-1)
        }
        else{
          LOGGER.info("覆盖模式数据开始处理")
          SparkTrans.process_overwrite(mode, sql, table_name)  
        }
        }
        //追加模式
      case "append" => {
        print("开启数据追加插入模式")
        if (args.length != 3) {
          print("输入参数错误，请重新输入")
          LOGGER.error("输入参数错误，请重新输入")
          System.exit(-1)
        }
        else{
          LOGGER.info("覆盖模式数据开始处理")
          SparkTrans.process_overwrite(mode, sql, table_name)  
        }
        }
      
      // 分区模式
      case "partition" => {
        print("开启数据覆盖插入模式")
        if (args.length != 3) {
          print("输入参数错误，请重新输入")
          LOGGER.error("输入参数错误，请重新输入")
          System.exit(-1)
        }
        else{
          LOGGER.info("覆盖模式数据开始处理")
          SparkTrans.process_overwrite(mode, sql, table_name)  
        }
        }

      }
  
  
  
    
  }
}