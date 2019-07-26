package net.ccic.sparkproess

import java.io.{FileInputStream, FileNotFoundException}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import ccic.net.bi.udf.define_ccic_udf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 目标：操作parquet
  * 功能：备份--backup
  *     删除--remove
  *     写入--overwrite+append
  *     合并--merged
  *     直接执行hsql
  * 修改历史：
  * 20171010   将读取路径更改为读取hive中的表
  */
object SparkEtl {
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  private final val LOGGER = Logger.getLogger(SparkEtl.getClass)
  private final val p = new Properties()
  private final var MASTER_URL = ""  // for prod


  //获取当前SparkSession
  val spark = SparkSession
    .builder
    .appName("s ${this.getClass.getSimpleName}")
    .enableHiveSupport()
    .getOrCreate()
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  //注册udf函数
  val sudf = new define_ccic_udf()
  sudf.init(spark)
  sudf.registerUDF()
 
  // main函数
  def main(args: Array[String]): Unit = {


    if (args.length == 0) {
      printMessage("请输入必要参数")
    }

    val cfgPath1 = args(0)
    loadProperties(cfgPath1)
    MASTER_URL = p.getProperty("master.url")
    println( "MASTER_URL: "+ MASTER_URL)
    val mode = args(1)
    
    mode match {
      //备份
      case "backup" => {
        printMessage("开启备份模式")
        if (args.length < 4) {
          printMessage("备份参数不够")
          LOGGER.error("备份参数不够")
          System.exit(-1)
        }
        val pathDir = args(2)
        val tableName = args(3)
        if (args.length == 4) {
          backupParquet(pathDir, tableName, null)
        } else if (args.length == 5) {
          if (!"".equals(args(4))) {
            backupParquet(pathDir, tableName, args(4))
          } else {
            backupParquet(pathDir, tableName, null)
          }

        }

      }


      //删除
      case "remove" => {
        printMessage("开启删除表模式")
        if (args.length < 4) {
          printMessage("删除参数不够")
          LOGGER.error("删除参数不够")
          System.exit(-1)
        }
        val pathDir = args(2)
        val tableName = args(3)
        removeParquet(pathDir, tableName)
      }

      //恢复之前表数据
      case "restore" => {
        printMessage("开启恢复表数据模式")
        if (args.length < 3) {
          printMessage("恢复参数不够")
          LOGGER.error("恢复参数不够")
          System.exit(-1)
        }
        val pathDir = args(2)
        val restoreTable = args(3)
        if (args.length == 4) {
          restoreParquet(pathDir, restoreTable, null)
        } else if (args.length == 5) {
          val backupTable = args(4)
          restoreParquet(pathDir, restoreTable, backupTable)
        }

      }

      //生成新表
      case "process_overwrite" => {
        printMessage("开启处理表数据(覆盖)模式")
        if (args.length < 4) {
          printMessage("处理参数不够")
          LOGGER.error("处理参数不够")
          System.exit(-1)
        }
        val sql = args(2)
        val outputName = args(3)
        processParquet(sql, outputName, "0")
      }

      //追加表之后
      case "process_append" => {
        printMessage("开启处理表数据(追加)模式")
        if (args.length < 4) {
          printMessage("处理参数不够")
          LOGGER.error("处理参数不够")
          System.exit(-1)
        }
        val sql = args(2)
        val outputName = args(3)
        processParquet(sql, outputName, "1")

      }

      //merge表数据
      case "merge_by_name" => {
        printMessage("开启合并数据模式")
        if (args.length < 5) {
          printMessage("合并参数不够")
          LOGGER.error("合并参数不够")
          System.exit(-1)
        }
        val baseTableName = args(2)
        val updateTableName = args(3)
        val keys = args(4)
        mergeParquet(baseTableName, updateTableName, keys, "byName")
      }
      //merge表数据
      case "merge_by_sql" => {
        printMessage("开启合并数据模式")
        if (args.length < 5) {
          printMessage("合并参数不够")
          LOGGER.error("合并参数不够")
          System.exit(-1)
        }
        val baseTableName = args(2)
        val sql = args(3)
        val keys = args(4)
        mergeParquet(baseTableName, sql, keys, "bySql")
      }

      //查询结果
      case "randomSQL" => {
        printMessage("执行临时的sql查询或执行模式")
        if (args.length < 3) {
          printMessage("处理参数不够")
          LOGGER.error("处理参数不够")
          System.exit(-1)
        }
        val sql = args(2)
        executeSQL(sql)

      }

      case _ => {
        printMessage("无此选项：" + mode)
      }

    }
  }

  /**
    * 功能：备份指定parquet文件
    * 参数：pathDir 读取和存储目录
    *     tableName 需要备份的表名
    *     [bkupTableName 指定备份表名,默认为"${tableName}.tmp"]
    */
  def backupParquet(pathDir: String, tableName: String, bkupTableName: String) = {
    var isSucceed = 0;
    var msg = ""
    val inPath = MASTER_URL + pathDir + "/" + tableName
    var outPath = ""
    if (bkupTableName == null || "".equals(bkupTableName)) {
      outPath = MASTER_URL + pathDir + "/" + tableName + ".bk"
    } else {
      outPath = MASTER_URL + pathDir + "/" + bkupTableName
    }
    LOGGER.info("开始备份，文件名为" + inPath + ",备份位置为" + outPath)
    println(sdf.format(new Date) + " | " + "开始备份，文件名为" + inPath + ",备份位置为" + outPath)

    try {
      //检查是否存在原始文件
      if (checkExists(inPath)) { //存在原始文件，进行正常备份
        val data = spark.read.parquet(inPath)
        val in_count = data.count()
        println(sdf.format(new Date) + " | " + "原始文件的记录数为" + in_count)
        data.write.mode(SaveMode.Overwrite).save(outPath)
        val out_count = spark.read.parquet(outPath).count()
        println(sdf.format(new Date) + " | " + "备份文件的记录数为" + out_count)
        println(sdf.format(new Date) + " | " + "备份完成！")
        msg = "完成备份！"
      } else {
        throw new FileNotFoundException("文件不存在：" + inPath)
      }

    } catch {
      case fnf: FileNotFoundException => {
        LOGGER.error("备份出现异常")
        LOGGER.error("文件不存在:" + fnf.getMessage)
        fnf.printStackTrace()
        isSucceed = 1
        msg = fnf.getMessage
      }
      case th: Throwable => {
        LOGGER.error("备份出现异常")
        LOGGER.error("异常信息为" + th.getMessage)
        th.printStackTrace()
        isSucceed = 1
        msg = th.getMessage
      }
    } finally {
      (isSucceed, msg)
    }

  }

  /**
    * 功能：删除表
    * 参数：pathDir 读取或存储hdfs位置
    * 			tableName 指定表名
    */
  def removeParquet(pathDir: String, tableName: String) {
    var isSucceed = 0;
    var msg = ""
    val delPath = MASTER_URL + pathDir + "/" + tableName
    val hdfsDelPath = new Path(delPath)
    val conf = new Configuration
    val fs = hdfsDelPath.getFileSystem(conf)
    printMessage("开始删除hdfs文件:" + delPath)
    if (fs.exists(hdfsDelPath)) {
      fs.delete(hdfsDelPath)
    }
    printMessage("删除hdfs文件:" + delPath + " 成功！")

  }

  /**
    * 功能：使用spark sql处理数据
    */
  def processParquet(sql: String, outputName: String, saveMode: String) {
    printMessage("开始处理数据")
    printMessage("处理的sql语句是" + sql)
    //执行sql语句
    val tablename = outputName.split("\\.")(1)
    println("tablename: " + tablename)
    val outData = spark.sql(sql)
    //outData.createOrReplaceTempView(tablename+"_tmp")
    printMessage("处理前的记录数为：" + outData.count())
    //保存输出数据
    printMessage("保存输出数据")
    printMessage("保存数据格式" + (if (saveMode == "0") "覆盖" else "追加"))
    //val path = MASTER_URL + PROCESS_PATH + "/" + outputName;
    if ("0".equals(saveMode)) { //覆盖
      //outData.write.mode(SaveMode.Overwrite).save(path)
      try {
        spark.table(outputName)
        //如果hive中已经存在output表，那么insert overwirte
        outData.write.mode(SaveMode.Overwrite).saveAsTable(outputName + "_tmp")
        spark.sql(s"INSERT OVERWRITE TABLE ${outputName} SELECT * FROM ${outputName}_tmp")
        spark.sql(s"DROP TABLE ${outputName}_tmp")
      } catch {
        case nst: NoSuchTableException => //如果hive不存在output表，新建一个
          outData.write.mode(SaveMode.Overwrite).saveAsTable(outputName)
      }finally{
        spark.sql(s"DROP TABLE IF EXISTS ${outputName}_tmp")
      }

    } else if ("1".equals(saveMode)) { //追加
      try {
        spark.table(outputName)
        //如果hive中已经存在output表，那么insert into
        //outData.write.mode(SaveMode.Append).save(path)
        printMessage("将新增数据追加到原来output表后面")
        outData.write.mode(SaveMode.Overwrite).saveAsTable(outputName + "_tmp")
        spark.sql("INSERT INTO TABLE ${outputName} SELECT * FROM ${outputName}_tmp")
        spark.sql("DROP TABLE ${outputName}_tmp")
      } catch {
        case nst: NoSuchTableException => //如果hive不存在output表，直接覆盖
          printMessage("output表不存在，新建一个，并将数据存入表中")
          outData.write.mode(SaveMode.Overwrite).saveAsTable(outputName)
      }finally{
        spark.sql("DROP TABLE IF EXISTS ${outputName}_tmp")
      }

    }
    printMessage("处理后的记录数为：" + spark.table(outputName).count())

    printMessage("处理数据完成，并保存")

  }

  /**
    * 功能：将更新数据的表和原表进行merge
    */
  def mergeParquet(baseFullname: String, update: String, keys: String, by: String) {
    LOGGER.info("开始合并数据")
    //val basePath = loadSingleData(baseFullname, spark)
    val baseData = spark.table(baseFullname)
    baseData.createOrReplaceTempView("old_data")
    LOGGER.info("基本表的记录数为：" + baseData.count())
    //    var updatePath:String = null
    var updateData: DataFrame = null
    if ("byName".equals(by)) {
      updateData = spark.table(update)
    } else if ("bySql".equals(by)) {
      //执行sql语句
      updateData = spark.sql(update)
    }
    updateData.createOrReplaceTempView("update_data")
    LOGGER.info("更新表的记录数为：" + updateData.count())

    val structType = baseData.schema
    //拼接select字段
    val oldSelect1 = new StringBuffer
    val otherSelect1 = new StringBuffer
    for (i <- 0 until structType.length) {
      if (!"_c1".equals(structType(i).name) && !"_other_indicator".equalsIgnoreCase(structType(i).name) && !"ym_copy".equals(structType(i).name)) {
        oldSelect1.append("old." + structType(i).name)
        otherSelect1.append("other." + structType(i).name)
        oldSelect1.append(",")
        otherSelect1.append(",")
      }
    }
    val lastOldCommaIndex = oldSelect1.lastIndexOf(",")
    val lastOtherCommaIndex = otherSelect1.lastIndexOf(",")
    val oldSelectStr = oldSelect1.substring(0, lastOldCommaIndex)
    val otherSelectStr = otherSelect1.substring(0, lastOtherCommaIndex)
    //拼接keys
    val keyStr = keys.split(",")
    //选第一个key作为判断记录是否需要更新的标志
    val indicator = keyStr(0)
    val keyrel = new StringBuffer
    for (i <- 0 until keyStr.length) {
      keyrel.append("old.").append(keyStr(i)).append("=other.").append(keyStr(i))
      if (i < keyStr.length - 1) {
        keyrel.append(" and ")
      }
    }
    println(keyrel)

    try {
      spark.sql(s"select $oldSelectStr , _other_indicator  from old_data old left join (select $otherSelectStr ,other.${indicator} as _other_indicator from update_data other) other on $keyrel ").filter("_other_indicator is null").createOrReplaceTempView("Tmp_All_data")
      spark.sql(s"select $oldSelectStr  from Tmp_All_data old where  _other_indicator is null").union(spark.sql(s"select $otherSelectStr from update_data other")).createOrReplaceTempView("merged_data")
      val outdata = spark.sql("select * from merged_data old")
      //outdata.coalesce(10).write.mode(SaveMode.Overwrite).saveAsTable(baseFullname)
      printMessage("合并数据到基表")
      outdata.write.mode(SaveMode.Overwrite).saveAsTable(baseFullname+"_tmp")
      spark.sql(s"INSERT OVERWRITE TABLE ${baseFullname} SELECT * FROM ${baseFullname}_tmp")
      spark.sql(s"DROP TABLE ${baseFullname}_tmp")
      printMessage("merge之后的记录数为：" + outdata.count())
      //renameData(outpath, basePath)
    }catch{
      case e: Exception =>{
        e.printStackTrace()
      }
    }finally{
      spark.sql(s"DROP TABLE IF EXISTS ${baseFullname}_tmp")
    }


  }

  def executeSQL(sql: String) = {
    printMessage("开始执行sql语句。默认展示20条")
    spark.sql(sql).show(20)
  }

  /**
    * 功能：处理过程失败时，恢复之前的数据
    */
  def restoreParquet(pathDir: String, restoreTable: String, backupTable: String): String = {
    printMessage("开始恢复parquet文件")
    val restorePath = MASTER_URL + pathDir + "/" + restoreTable
    var backupPath = ""
    if (backupTable == null || "".equals(backupTable)) {
      backupPath = MASTER_URL + pathDir + "/" + restoreTable + ".bk"
    } else {
      backupPath = MASTER_URL + pathDir + "/" + backupTable
    }
    printMessage("恢复的parquet文件为：" + restorePath)

    val conf = new Configuration()
    val oldPath = new Path(backupPath)
    val newPath = new Path(restorePath)
    val fs = newPath.getFileSystem(conf)
    if (fs.exists(newPath)) {
      printMessage("删除parquet文件：" + restorePath)
      fs.delete(newPath)
    }
    if (fs.exists(oldPath)) {
      printMessage("从 " + backupPath + " 恢复parquet文件:" + restorePath)
      val isok = fs.rename(oldPath, newPath)
    } else {
      printMessage("无备份数据可以恢复：" + backupPath)
    }
    return backupPath

  }


  def loadProperties(cfgPath:String) {
    //val appStream = getClass()
      //.getClassLoader()
      //.getResourceAsStream(cfgPath)
      //.getResourceAsStream("/home/bigdata/cfg/processing.properties")
      //p.load(appStream)
    val appStream = new FileInputStream(cfgPath)
    p.load(appStream)
  }

  /**
    * 功能：将sql语句中的表名替换成真正的表名
    * 参数：sql 需要parse的sql语句
    * 		 tables 读入的表
    * 返回值： 替换后的sql语句
    */
  def parseSql(sql: String, tables: java.util.List[String]): String = {
    var fixedSql = sql
    for (i <- 0 until tables.size()) {
      val tableInfo = tables.get(i).split("\\.")
      if (tableInfo != null && tableInfo.length > 1) {
        val fullTableName = tables.get(i)
        println(fullTableName)
        fixedSql = fixedSql.replaceAll(fullTableName, tableInfo(1))
      } else {
        fixedSql = fixedSql.replaceAll(tables.get(i), tableInfo(0))
      }
    }
    println(fixedSql)
    return fixedSql
  }

  /**
    * 检查指定文件是否存在
    */
  def checkExists(filePath: String): Boolean = {
    printMessage("开始检查指定文件是否存在：" + filePath)
    val conf = new Configuration()
    val checkPath = new Path(filePath)
    val fs = checkPath.getFileSystem(conf)
    return fs.exists(checkPath)
  }

  def renameData(oldPath: String, newPath: String) {
    val conf = new Configuration();
    val oldDfsPath = new Path(oldPath)
    val fs = oldDfsPath.getFileSystem(conf)
    val replacePath = new Path(newPath)
    if (fs.exists(oldDfsPath) && fs.exists(replacePath)) {
      println("删除目录" + replacePath.getName)
      fs.delete(replacePath)
    }
    if (fs.exists(oldDfsPath)) {
      println("将数据目录更名为" + replacePath.getName)
      val isok = fs.rename(oldDfsPath, replacePath);
    }
  }

  /**
    * 打印信息至控制台
    */
  private def printMessage(msg: String) {
    println(sdf.format(new Date) + " | " + msg)
  }

}
