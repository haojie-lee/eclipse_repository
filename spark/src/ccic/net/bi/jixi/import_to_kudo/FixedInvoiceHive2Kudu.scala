package bmsoft.ccic.adhoc.main

import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructField, StructType}

import scala.collection.JavaConverters._
import org.slf4j.LoggerFactory


object FixedInvoiceHive2Kudu {
  val LOG = LoggerFactory.getLogger(FixedInvoiceHive2Kudu.getClass)
  /**
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    require(args.length == 4)
    importToKUDU(args(0), args(1), args(2), args(3))
  }


  /**
    * Import data from HiVE to Kudu.
    * @param kuduMasters
    * @param query
    * @param targetTable Kudu table.
    */
  def importToKUDU(mode: String, query: String, kuduMasters: String, targetTable: String): Unit = {

    try {
      mode match {
        case "ALL" => importInitData(query: String, kuduMasters: String, targetTable: String)
        case "INC"=> importIncrementalData(query: String, kuduMasters: String, targetTable: String)
        case _ => LOG.error("Unknown exec mode!")
      }
    } catch {
      case exception: Exception => LOG.error("Import data error:" + exception.getMessage, exception)
    }
  }

  def importInitData(query: String, kuduMasters: String, targetTable: String): Unit = {
    LOG.info("The kudu mastes str is:" + kuduMasters)
    LOG.info("Get spark session instance.")
    val sparkSession = getSparkSession()
    import sparkSession.sql
    LOG.info("Get Kudu context instance.")
    val masters = kuduMasters.toString.split(",").toSeq.mkString(",")
    val kuduContext = getKuduContext(masters, sparkSession.sparkContext)
    LOG.info("Query data from hive table.")

    try {
      val dataFrame = sql(query)
        val kuduDataFrame = dataFrame.withColumn("id", monotonically_increasing_id)
        kuduContext.insertIgnoreRows(kuduDataFrame, targetTable)
    } catch {
      case exception: Exception => LOG.error("Import data error:" + exception.getMessage, exception)
    } finally {
      sparkSession.close()
    }

  }

  def importIncrementalData(query: String, kuduMasters: String, kuduTable: String): Unit = {

    LOG.info("Get spark session instance.")
    val sparkSession = getSparkSession()

    import sparkSession.sql
    LOG.info("Get Kudu context instance.")
    val masters = kuduMasters.toString.split(",").toSeq.mkString(",")

    val kuduContext = getKuduContext(masters, sparkSession.sparkContext)

    try {
      LOG.info("Query data from hive table.")
      val dataFrame = sql(query)

        if (kuduContext.tableExists(kuduTable)) {
          LOG.info("Delete the table:" + kuduTable)
          kuduContext.deleteTable(kuduTable)
        }

        // Define the primary key
        val kuduPrimaryKey = Seq("ID")

        // Define a schema
        val rawTableFields = dataFrame.schema.fields

        LOG.info("The raw table schema is:" + dataFrame.schema)
        val kuduFields = rawTableFields.+:(StructField("ID", LongType, false))

        val kuduTableSchema = StructType(kuduFields)

        // Specify any further options
        val kuduTableOptions = new CreateTableOptions()
        kuduTableOptions.addHashPartitions(List("ID").asJava,20)

        // Call create table API
        kuduContext.createTable(kuduTable, kuduTableSchema, kuduPrimaryKey, kuduTableOptions)
        LOG.info("Created [" + kuduTable + "] Successful.")

        val kuduDataFrame = dataFrame.withColumn("ID", monotonically_increasing_id())
        LOG.info("Begin to write data to kudu table.")
        kuduContext.insertIgnoreRows(kuduDataFrame, kuduTable)
    } catch {
      case exception: Exception => LOG.error("Import data error:" + exception.getMessage, exception)
    } finally {
      sparkSession.close()
    }
  }


  /**
    * The help information about how to use this tool.
    */
  def printUSAGE(): Unit = {
    val command =
      """|Usage:   SparkKuduETL [hive Query Statement] [Kudu Masters] [Impala Kudu Table] [zkhosts host:port]
         |Example: SparkKuduETL "SELECT name, address, phone FROM userinfo" "master1,master2" "impala::adhoc.users"
      """.stripMargin
    println(command)
  }


  /**
    * Get kudu context instance.
    * @param kudumasters
    * @return
    */
  def getKuduContext(kudumasters: String, sparkContext: SparkContext): KuduContext = {
    new KuduContext(kudumasters, sparkContext)
  }

  /**
    * Get a SparkSession instance, which support Hive query.
    * @return SparkSession
    */
  def getSparkSession(): SparkSession = {
    SparkSession.builder()
      .config("spark.driver.allowMultipleContexts", "true")
      .appName(s"${this.getClass.getSimpleName}")
      .enableHiveSupport()
      .getOrCreate()
  }

}
