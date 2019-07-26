package ccic.net.bi.sparkproject.etl

import java.io.FileNotFoundException
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import ccic.net.bi.spark.etl.SparkEtl
import net.ccic.sparkprocess.enter.SparkEtlFun
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
  * Created by FrankSen on 2018/7/2.
  */
object SparkEtlImpl extends SparkEtlFun{

  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  private final val LOGGER = Logger.getLogger(SparkEtl.getClass)
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  /**
    *
    * @param sql 插入分区类型SQL语句 如：
    *            alter table ccic_dev.test_partition_prpcmain_new
    *            add if not exists partition(workdate=20180620);
    *            insert overwrite table ccic_dev.test_partition_prpcmain_new
    *            partition(workdate=20180620)
    *            select
    *            policyno
    *            , startdate
    *            , enddate
    *            from ccic_source.t03_prpcmain
    */
  override def insertPartition(sql: String, spark: SparkSession): Unit = {

      val sqlArr = sql.split(";")  //

      for (i <- 0 until sqlArr.length){
        printMessage("开始处理数据")
        printMessage("处理的sql语句是:\n" + sqlArr(i))
        if (sqlArr(i).trim.isEmpty || sqlArr(i).trim.equals("")){
          printMessage("SQL语句为空，不执行。")
        }else{
            spark.sql(sqlArr(i))
        }
      }

  }

  /**
    * 功能：备份指定parquet文件
    * 参数：pathDir 读取和存储目录
    * tableName 需要备份的表名
    * [bkupTableName 指定备份表名,默认为"${tableName}.tmp"]
    */
  override def backupParquet(master_url: String, spark: SparkSession, pathDir: String, tableName: String, bkupTableName: String): Unit = {
    var isSucceed = 0
    var msg = ""
    val inPath = master_url + pathDir + "/" + tableName
    var outPath = ""
    if (bkupTableName == null || "".equals(bkupTableName)) {
      outPath = master_url + pathDir + "/" + tableName + ".bk"
    } else {
      outPath = master_url + pathDir + "/" + bkupTableName
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
      case fnf: FileNotFoundException =>
        LOGGER.error("备份出现异常")
        LOGGER.error("文件不存在:" + fnf.getMessage)
        fnf.printStackTrace()
        isSucceed = 1
        msg = fnf.getMessage
      case th: Throwable =>
        LOGGER.error("备份出现异常")
        LOGGER.error("异常信息为" + th.getMessage)
        th.printStackTrace()
        isSucceed = 1
        msg = th.getMessage
    } finally {
      (isSucceed, msg)
    }
  }

  /**
    * 功能：删除表
    * 参数：pathDir 读取或存储hdfs位置
    * tableName 指定表名
    */
  override def removeParquet(master_url: String, spark: SparkSession, pathDir: String, tableName: String): Unit = {
    var isSucceed = 0
    var msg = ""
    val delPath = master_url + pathDir + "/" + tableName
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
  override def processParquet(spark: SparkSession, sql: String, outputName: String, saveMode: String): Unit = {
    printMessage("开始处理数据")
    printMessage("处理的sql语句是:\n" + sql)
    val tableName = outputName.split("\\.")(1)
    println("tableName: " + tableName)
    val outData = spark.sql(sql)
    printMessage("保存输出数据")
    printMessage("保存数据格式" + (if (saveMode == "0") "覆盖" else "追加"))
    if ("0".equals(saveMode)) {
      //覆盖''
      try {
        spark.sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold","20971520")
        outData.createOrReplaceTempView(s"${tableName}_tmp_view")
        spark.sql(s"INSERT OVERWRITE TABLE $outputName SELECT * FROM ${tableName}_tmp_view")

      } catch {
        case nst: NoSuchTableException => //如果hive不存在output表，新建一个
          outData.write.mode(SaveMode.Overwrite).saveAsTable(outputName)
      }finally{
        spark.sql(s"DROP TABLE IF EXISTS ${tableName}_tmp_view")
      }
    } else if ("1".equals(saveMode)) { //追加
      try {
        spark.table(outputName)
        //如果hive中已经存在output表，那么insert into
        //outData.write.mode(SaveMode.Append).save(path)
        printMessage("将新增数据追加到原来output表后面")
        outData.createOrReplaceTempView(s"${tableName}_tmp_view")
        spark.sql(s"INSERT INTO TABLE $outputName SELECT * FROM ${tableName}_tmp_view")

      } catch {
        case nst: NoSuchTableException => //如果hive不存在output表，直接覆盖
          printMessage("output表不存在，新建一个，并将数据存入表中")
          outData.write.mode(SaveMode.Overwrite).saveAsTable(outputName)
      }finally{
        spark.sql(s"DROP TABLE IF EXISTS ${outputName}_tmp")
      }
    }
    printMessage("处理后的记录数为：" + spark.table(outputName).count())

    printMessage("处理数据完成，并保存")

  }

  /**
    * 功能：将更新数据的表和原表进行merge
    */
  override def mergeParquet(spark: SparkSession, baseFullname: String, update: String, keys: String, by: String): Unit = {

    LOGGER.info("开始合并数据")
    //val basePath = loadSingleData(baseFullname, spark)
    //将物理表转化为dataframe，进而转化为临时视图
    val baseData = spark.table(baseFullname)
    baseData.createOrReplaceTempView("old_data")
    //LOGGER.info("基本表的记录数为：" + baseData.count())
    //    var updatePath:String = null
    var updateData: DataFrame = null
    if ("byName".equals(by)) {
      updateData = spark.table(update)
    } else if ("bySql".equals(by)) {
      //执行sql语句
      updateData = spark.sql(update).persist(StorageLevel.MEMORY_AND_DISK)
    }

    updateData.createOrReplaceTempView("update_data")
    //LOGGER.info("更新表的记录数为：" + updateData.count())

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
    //去除拼接的字段的最后一个","
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
    printMessage(s"""Merge SQL: with tab as(select $oldSelectStr
                    |, _other_indicator
                    |from old_data old
                    |left join (select $otherSelectStr
                    |            ,other.$indicator as _other_indicator
                    |            from update_data other
                    |          ) other
                    |on $keyrel)
                    |select * from tab
                    |Relation Key: $keyrel
                    |""")

    try {

      val selectStr  = oldSelectStr.replaceAll("old\\.","").split(",")
      import org.apache.spark.sql.functions.col
      spark.sql(
        s"""
           |select $oldSelectStr
           |, _other_indicator
           |from old_data old
           |left join (select $otherSelectStr
           |            ,other.$indicator as _other_indicator
           |            from update_data other
           |          ) other
           |on $keyrel
         """.stripMargin).filter("_other_indicator is null").select(selectStr.map(c => col(c)):_*)
        .union(spark.sql(s"select $otherSelectStr from update_data other"))
        .repartition(keyStr.map(key => col(key)):_*)
        .write.mode(SaveMode.Overwrite).saveAsTable(baseFullname+"_merge_tmp")

      printMessage("合并数据到基表")
      spark.sql(s"INSERT OVERWRITE TABLE $baseFullname SELECT * FROM ${baseFullname}_merge_tmp")
      //printMessage("merge之后的记录数为：" + outdata.count())
    }catch{
      case e: Exception =>{
        e.printStackTrace()
      }
    }finally {
      spark.sql(s"drop table if exists ${baseFullname}_merge_tmp ")
    }
  }

  /**
    * 功能：处理过程失败时，恢复之前的数据
    */
  override def restoreParquet(master_url: String, spark: SparkSession, pathDir: String, restoreTable: String, backupTable: String): String = {
    printMessage("开始恢复parquet文件")
    val restorePath = master_url + pathDir + "/" + restoreTable
    var backupPath = ""
    if (backupTable == null || "".equals(backupTable)) {
      backupPath = master_url + pathDir + "/" + restoreTable + ".bk"
    } else {
      backupPath = master_url + pathDir + "/" + backupTable
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
    backupPath
  }

  /**
    * 功能：将sql语句中的表名替换成真正的表名
    * 参数：sql 需要parse的sql语句
    * tables 读入的表
    * 返回值： 替换后的sql语句
    */
  override def parseSql(spark: SparkSession, sql: String, tables: util.List[String]): String = {
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
    printMessage(fixedSql)
    fixedSql
  }


  /**
    * 输出日志
    * @param message
    */
  def printMessage(message: String): Unit ={
    println(sdf.format(new Date) + " | " + message)
  }

  /**
    * 检查指定文件是否存在
    */
  def checkExists(filePath: String): Boolean = {
    printMessage("开始检查指定文件是否存在：" + filePath)
    val conf = new Configuration()
    val checkPath = new Path(filePath)
    val fs = checkPath.getFileSystem(conf)
    fs.exists(checkPath)
  }
}
