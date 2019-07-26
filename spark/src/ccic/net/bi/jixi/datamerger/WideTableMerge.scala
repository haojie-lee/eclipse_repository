package ccic.net.bi.jixi.datamerger

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions._

object WideTableMerge {
  val LOG = LoggerFactory.getLogger(WideTableMerge.getClass)

  def main(args: Array[String]): Unit = {
    require(args.length == 4)
    LOG.info("The parameters are:" + args.toString)
    merge(args(0), args(1), args(2), args(3))
  }

  /**
    *
    * @param src The update data table.
    * @param target The major data table.
    * @param trackTable The track table, records the update data.
    * @param mergeKeys The merge keys.
    */
  def merge(updateTable: String, targetTable: String, trackTable:String, mergeKeys: String): Unit = {
    LOG.info("Begin to merge data.")
    val sparkSession = getSparkSession()
    LOG.info("Get spark session instance")

    import sparkSession.sql

    val mergeTable = targetTable + "_MERGE"
    val majorAlias = "major"
    val majorTempView = majorAlias + "_data"
    val incAlias = "incremental"
    val incTempView = incAlias + "_data"

    try {

      val incremental_data = sparkSession.table(updateTable)

      LOG.info("Get incremnt data from " + updateTable )

      // 获取增量数据添加时间戳之后的字段
      val structType = incremental_data.schema
      LOG.info("The structy type is:" + structType)

      // 拼接select字段
      val majorSelectFields = new StringBuffer
      val incrementSelect = new StringBuffer
      for (i <- 0 until structType.length) {
        majorSelectFields.append(majorAlias + "." + structType(i).name)
        incrementSelect.append(incAlias + "." + structType(i).name)
        majorSelectFields.append(",")
        incrementSelect.append(",")
      }

      // 拼接额外字段
      val majorSelectStr = majorSelectFields.append(majorAlias + ".data_update_time,").append(majorAlias + ".data_edit_time,")
        .append(majorAlias + ".orig_etl_systime")

      val lastOtherCommaIndex = incrementSelect.lastIndexOf(",")
      val incrementSelectStr = incrementSelect.substring(0, lastOtherCommaIndex)

      // 拼接条件
      val keyStr = mergeKeys.split(",")
      val keyrel = new StringBuffer
      for (i <- 0 until keyStr.length) {
        keyrel.append(majorAlias + ".").append(keyStr(i)).append(" = " + incAlias + ".").append(keyStr(i))
        if (i < keyStr.length - 1) {
          keyrel.append(" and ")
        }
      }

      LOG.info("The condition is:" + keyrel)

      // 如果track table的值不为NULL就写入track表
      if (!trackTable.equalsIgnoreCase("NULL")) {
        val result = incremental_data.withColumn("data_update_time", current_timestamp())
          .withColumn("data_edit_time", current_timestamp())
          .withColumn("orig_etl_systime", current_timestamp())
          .withColumn("etl_merge_date", current_timestamp())
        LOG.info(s"Writing update data to track table.")
        result.write.mode(SaveMode.Append).insertInto(trackTable)
      }

      // 注册临时表
      sql(s"SELECT $majorSelectStr FROM $targetTable $majorAlias").createOrReplaceTempView(majorTempView)
      incremental_data.createOrReplaceTempView(incTempView)

      // 关联出主表中不存在于增量表中的数据
      sql(s"SELECT $majorSelectStr FROM $majorTempView $majorAlias left anti join $incTempView $incAlias on $keyrel")
        .createOrReplaceTempView("tmp_all_data")

      val majorData = sql("SELECT * FROM tmp_all_data").toDF().persist()


      LOG.info("Before overwrite the total number is:" + majorData.count())

      // 为增量数据添加时间字段并赋值
      val updateData = sql(s"SELECT * FROM $incTempView").toDF()
      val updateDataWithTimestamp = updateData.withColumn("data_update_time", current_timestamp())
        .withColumn("data_edit_time", current_timestamp())
        .withColumn("orig_etl_systime", current_timestamp())

      // 用增量数据overwrite源表数据
      updateDataWithTimestamp.write.format("PARQUET").mode(SaveMode.Overwrite).insertInto(targetTable)

      LOG.info("After overwrite the total num is:" + majorData.count())

      // 追加数据到主表
      majorData.write.mode(SaveMode.Append).insertInto(targetTable)

    } catch {
      case exception: Exception => LOG.error("Merge error:" + exception.getMessage, exception)
    } finally {
      // Shutdown and close.
      sparkSession.close()
    }
  }

  /**
    * Get a SparkSession instance, which support Hive query.
    * @return SparkSession
    */
  def getSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName(s"${this.getClass.getSimpleName}")
      .config("spark.driver.allowMultipleContexts","true")
      .enableHiveSupport()
      .getOrCreate()
  }

  /**
    * The help information about how to use this tool.
    */
  def printUSAGE(): Unit = {
    val command =
      """|Usage:   SparkHiveETL [update table] [major table] [truck table] [merge keys]
         |Example: SparkHiveETL "test_update_temp" "test" "test_truck" "ID"
      """.stripMargin
    println(command)
  }
}

