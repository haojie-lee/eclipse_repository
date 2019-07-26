package ccic.net.bi.jixi.import_to_kudo


import scala.collection.JavaConverters._
import org.apache.kudu.client.{CreateTableOptions, KuduSession}
import org.apache.kudu.client.KuduClient.KuduClientBuilder
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import org.apache.kudu.client.KuduClient
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.kudu.spark.kudu.KuduContext

object WideTableHive2Kudu {

  val LOG = LoggerFactory.getLogger(WideTableHive2Kudu.getClass)

  def main(args: Array[String]): Unit = {
    require(args.length > 0)
    importToKUDU(args(0), args(1), args(2), args(3))
  }

  /**
    * Import data from HiVE to Kudu.
    * @param kuduMasters
    * @param query
    * @param targetTable Kudu table.
    */
  def importToKUDU(mode: String, query: String, kuduMasters: String, targetTable: String):Unit = {
    LOG.info("The kudu mastes str is:" + kuduMasters)
    LOG.info("Get spark session instance.")
    val sparkSession = getSparkSession()
    LOG.info("Get Kudu context instance.")
    val masters = kuduMasters.toString.split(",").toSeq.mkString(",")
    val kuduContext = getKuduContext(masters, sparkSession.sparkContext)

    try {
      LOG.info("Query data from hive table.")
      val dataFrame = sparkSession.sql(query)

      mode match {
        case "ALL" => importFullData(query, kuduMasters, targetTable)
        case "INC" => kuduContext.upsertRows(dataFrame, targetTable)
        case "TRACK" => kuduContext.insertIgnoreRows(dataFrame, targetTable)
        case _ => LOG.error("Unknown exec mode!")
      }
    } catch {
      case exception: Exception => LOG.error("Import data error:" + exception.getMessage, exception)
    } finally {
      sparkSession.close()
     }
  }


  def importFullData(query: String, kuduMasters: String, kuduTable: String):Unit = {
    LOG.info("Get spark session instance.")
    val sparkSession = getSparkSession()

    import sparkSession.sql
    LOG.info("Get Kudu context instance.")
    val masters = kuduMasters.toString.split(",").toSeq.mkString(",")

    val kuduContext = getKuduContext(masters, sparkSession.sparkContext)

    val kuduClient  = new KuduClientBuilder(kuduMasters).build()

    try {
      LOG.info("Query data from hive table.")
      val dataFrame = sql(query)
      
      
      if (kuduContext.tableExists(kuduTable)) {
        // Get a kudu table schema from the exists table.
        val kuduSchema  = kuduClient.openTable(kuduTable).getSchema
        LOG.info("Deleting the table:" + kuduTable)
        kuduContext.deleteTable(kuduTable)
        LOG.info("Deleted table [" + kuduTable + "] succeed.")
        LOG.info("Prepare kudu schema for recreating.")
        val primaryKeySchema = kuduSchema.getPrimaryKeyColumns.asScala
        val primaryKeys = for (primaryKey <- primaryKeySchema) yield primaryKey.getName
        val createTableOptions = new CreateTableOptions()
        LOG.info("Set default table hash partitions to 30.")
        createTableOptions.addHashPartitions(primaryKeys.asJava,30)
        kuduClient.createTable(kuduTable, kuduSchema, createTableOptions)
        LOG.info("Created table [" + kuduTable + "] Successful.")
        LOG.info("Begin to write data to kudu table.")
        kuduContext.insertIgnoreRows(dataFrame, kuduTable)
      } else {
        val fields = dataFrame.schema.fields

      }
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
      """|Usage:   SparkKuduETL [mode] [query] [kudumaster] [kudu table]
         |Example: SparkKuduETL "ALL" "select * from test" "master1,master2" "impala::test"
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
