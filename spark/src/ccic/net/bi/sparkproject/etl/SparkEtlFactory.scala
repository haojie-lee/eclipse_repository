package ccic.net.bi.sparkproject.etl

import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Properties
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import net.ccic.udf.define_ccic_udf


/**
  * Created by FrankSen on 2018/7/2.
  */
object SparkEtlFactory {


  val spark = SparkSession
    .builder
    //.appName(s"${this.getClass.getSimpleName}")
    .enableHiveSupport()
    //.master("local")
    .getOrCreate()

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val sudf = new define_ccic_udf()


  //注册临时静态函数
  sudf.init(spark)
  sudf.registerUDF()


  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  private final val LOGGER = Logger.getLogger(SparkEtlFactory.getClass)
  private final val p = new Properties()
  private final var MASTER_URL = ""

  def executeMatch(args: Array[String]): Unit = {
    loadProperties(args(0))
    MASTER_URL = p.getProperty("master.url")
    println( "MASTER_URL: "+ MASTER_URL)
    val mode = args(1)
    mode match {
      //覆盖原表overwrite
      case "process_overwrite" =>
        printMessage("开启处理表数据(覆盖)模式")
        if (args.length < 4) {
          printMessage("处理参数不够")
          LOGGER.error("处理参数不够")
          System.exit(-1)
        }
        val sql = args(2)
        val outputName = args(3)
        SparkEtlImpl.processParquet(spark, sql, outputName, "0")

      //追加表之后
      case "process_append" =>
        printMessage("开启处理表数据(追加)模式")
        if (args.length < 4) {
          printMessage("处理参数不够")
          LOGGER.error("处理参数不够")
          System.exit(-1)
        }
        val sql = args(2)
        val outputName = args(3)
        SparkEtlImpl.processParquet(spark, sql, outputName, "1")

      //插入到指定分区
      case "insertPartition" =>
        printMessage("插入数据到指定分区")
        if (args.length < 1) {
          printMessage("处理参数不够")
          LOGGER.error("处理参数不够")
          System.exit(-1)
        }
        val sql = args(2)
        SparkEtlImpl.insertPartition(sql, spark)

      case "backup" =>
        printMessage("开启备份模式")
        if (args.length < 4) {
          printMessage("备份参数不够")
          LOGGER.error("备份参数不够")
          System.exit(-1)
        }
        val pathDir = args(2)
        val tableName = args(3)
        if (args.length == 4) {
          SparkEtlImpl.backupParquet(MASTER_URL, spark, pathDir, tableName, null)
        } else if (args.length == 5) {
          if (!"".equals(args(4))) {
            SparkEtlImpl.backupParquet(MASTER_URL, spark, pathDir, tableName, args(4))
          } else {
            SparkEtlImpl.backupParquet(MASTER_URL, spark, pathDir, tableName, null)
          }

        }

      case "remove" =>
        printMessage("开启删除表模式")
        if (args.length < 4) {
          printMessage("删除参数不够")
          LOGGER.error("删除参数不够")
          System.exit(-1)
        }
        val pathDir = args(2)
        val tableName = args(3)
        SparkEtlImpl.removeParquet(MASTER_URL, spark, pathDir, tableName)

      //恢复之前表数据
      case "restore" =>
        printMessage("开启恢复表数据模式")
        if (args.length < 3) {
          printMessage("恢复参数不够")
          LOGGER.error("恢复参数不够")
          System.exit(-1)
        }
        val pathDir = args(2)
        val restoreTable = args(3)
        if (args.length == 4) {
          SparkEtlImpl.restoreParquet(MASTER_URL, spark, pathDir, restoreTable, null)
        } else if (args.length == 5) {
          val backupTable = args(4)
          SparkEtlImpl.restoreParquet(MASTER_URL, spark, pathDir, restoreTable, backupTable)
        }

        //merge表数据
      case "merge_by_name" =>
        printMessage("开启合并数据模式")
        if (args.length < 5) {
          printMessage("合并参数不够")
          LOGGER.error("合并参数不够")
          System.exit(-1)
        }
        val baseTableName = args(2)
        val updateTableName = args(3)
        val keys = args(4)
        SparkEtlImpl.mergeParquet(spark, baseTableName, updateTableName, keys, "byName")

      //merge表数据
      case "merge_by_sql" =>
        printMessage("开启合并数据模式")
        if (args.length < 5) {
          printMessage("合并参数不够")
          LOGGER.error("合并参数不够")
          System.exit(-1)
        }
        val baseTableName = args(2)
        val sql = args(3)
        val keys = args(4)
        SparkEtlImpl.mergeParquet(spark, baseTableName, sql, keys, "bySql")


      case _ =>
        printMessage("无此选项：" + mode)
    }
    spark.stop()
  }
    def printMessage(message: String): Unit = {
      println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " | " + message)
    }


    def loadProperties(cfgPath: String) {
      val appStream = new FileInputStream(cfgPath)
      p.load(appStream)
    }

}
