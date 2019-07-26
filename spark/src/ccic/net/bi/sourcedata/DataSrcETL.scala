package ccic.net.bi.sourcedata

import java.io.{FileInputStream, FileNotFoundException}
import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.types.StructType

/**
  * 数据源的导入管理，支持数据源的增删改查等操作
  *
  * status说明
  * 0:原有数据
  * 1:新增数据
  * 2:删除数据
  * 3:修改数据
  *
  */

class DataSrcETL(IDD: String) {
  val sqlContext = SparkSession
    .builder
    .appName(s"s_${IDD}")
    .config("spark.some.config.option", "some-value")
    .enableHiveSupport()
    .getOrCreate()

  /** ****************************************************************************************************************/
  def merge(DBID: String, rootPath: String, key: String, hdfsPath: String,collNN:String) {
    println(DateUtil.getDateTimeNow() + "DBID=" + DBID)

    //创建数据库连接
    val jdbcConnection = getJDBCConnection
    val sql = "select * from DBMANAGE where ID='" + DBID + "'"
    val rs = jdbcConnection.prepareStatement(sql).executeQuery()
    val collN=collNN.toInt
    var isUpdate = false
    var isdel = false
    var oldPath = ""
    var oldPathMerged = ""
    var incPath = ""
    var newPathMerged = ""
    //var readyUpdateDF: DataFrame = null
    //获取相关处理信息（表+数据库+数据路径）
    try {
      if (rs.next()) {
        val sourceKey = rs.getString("SOURCE_TABLE_KEY")
        val paritionKey = rs.getString("PARTITION_COL")
        var structTypeI: StructType = null
        var structTypeO: StructType = null
        //拼接主键，循环遍历主键，"and"拼接
        val keys = sourceKey.split(",")
        val keyrel = new StringBuffer
        for (i <- 0 until keys.length) {
          keyrel.append("old.").append(keys(i)).append("=inc.").append(keys(i))
          if (i < keys.length - 1) {
            keyrel.append(" and ")
          }
        }
        //val keyrel2 = "old." + keys(0) + ">=" + "\"" + bedate1 + "\"" + " and " + "old." + keys(0) + "<" + "\"" + endate1 + "\""

        println(DateUtil.getDateTimeNow() + keyrel)
        //println(DateUtil.getDateTimeNow() + keyrel2)
        //从数据库中获得全量字段、表名和数据库名
        val isFullImport = rs.getString("isFullImport")
        val sourceTable = rs.getString("SOURCE_TABLE").trim()
        val dataBaseName = rs.getString("DATABASE_NAME").trim()

        //拼接存储RDB数据的hdfs路径
        oldPath = hdfsPath + rootPath + dataBaseName + "/" + sourceTable
        oldPathMerged = hdfsPath + rootPath + dataBaseName + "/" + sourceTable + "/merged"
        newPathMerged = hdfsPath + rootPath + dataBaseName + "/" + sourceTable + "/merged" + "/new"

        /** ************************************************************************************************************/
        //检查merged文件夹是否存在,判断是否处理数据
        println(DateUtil.getDateTimeNow() + "检查merged文件夹是否存在：" + oldPathMerged)
        if (checkReady(oldPathMerged)) {
          println(DateUtil.getDateTimeNow() + "merged文件夹存在：" + oldPathMerged)

          //根据isFullImport字段判断数据处理逻辑；为"0"表示全量更新
          if ("0".equals(isFullImport)) {
            val conf = new Configuration();
            val oldAllPathOld = new Path(oldPath + "/all.old")
            val oldAllPathA = new Path(oldPath + "/all")
            val oldMergedAllPathB = new Path(oldPathMerged + "/all")
            val fs = oldAllPathOld.getFileSystem(conf)
            if (fs.exists(oldAllPathOld)) {
              fs.delete(oldAllPathOld);
            }
            fs.rename(oldAllPathA, oldAllPathOld);
            fs.rename(oldMergedAllPathB, oldAllPathA);
          } else {
            if ("1".equals(isFullImport)) {
              incPath = oldPathMerged + "/modify";
            } else if ("2".equals(isFullImport)) {
              incPath = oldPathMerged + "/add";
            } else if ("3".equals(isFullImport)) {
              incPath = oldPathMerged + "/del";
            } else {
              println(DateUtil.getDateTimeNow() + "未知状态");
              System.exit(1)
            }
            val incData = loadData(incPath)

            //check是否存在all文件
            val conf = new Configuration();
            val oldAllPath = new Path(oldPath + "/all")
            var oldData = loadData(oldPath + "/all")
            val oldAllPathOld = new Path(oldPath + "/all.old")
            val oldAllPathOldMv = new Path(oldPath + "/all.old.mv")
            val fs = oldAllPath.getFileSystem(conf)
            //var oldData: DataFrame = null
            if (fs.exists(oldAllPathOld) && !(oldData != null)) {
              println(DateUtil.getDateTimeNow() + "All数据不存在，恢复数据...")
              val isok = fs.rename(oldAllPathOld, oldAllPath);
              println(DateUtil.getDateTimeNow() + "恢复数据完成。")
              oldData = loadData(oldPath + "/all")
            }
            //非全量导入更新数据
            if ("1".equals(isFullImport)) {
              incPath = oldPathMerged + "/modify";
            } else if ("2".equals(isFullImport)) {
              incPath = oldPathMerged + "/add";
            } else if ("3".equals(isFullImport)) {
              incPath = oldPathMerged + "/del";
            } else {
              println(DateUtil.getDateTimeNow() + "未知状态");
              //return 1
            }
            //字段比较获取最新字段信息

            //incData.repartition(collN).createOrReplaceTempView("inc_data")
            //sqlContext.sql("CACHE TABLE inc_data")
            incData.createOrReplaceTempView("inc_data")
            structTypeI = incData.schema
            oldData.createOrReplaceTempView("old_data")
            structTypeO = oldData.schema
            val lengthO = structTypeO.length
            val lengthI = structTypeI.length
            val lengthMi = lengthI - lengthO
            val checkSelect = new StringBuffer
            val incSelect1 = new StringBuffer
            val incSelect2 = new StringBuffer
            val oldSelect1 = new StringBuffer
            val oldSelect2 = new StringBuffer
            var allSelectStr1 = ""
            var allSelectStr2 = ""
            if (lengthMi < 0) {
              println(DateUtil.getDateTimeNow() + "字段个数异常");
              println(DateUtil.getDateTimeNow() + "原字段" + lengthO);
              println(DateUtil.getDateTimeNow() + "新字段" + lengthI);
              System.exit(1)
            } else {
              for (i <- 0 until structTypeO.length) {
                if (i == 0) {
                  oldSelect1.append("old." + structTypeO(i).name.toUpperCase)
                } else {
                  oldSelect1.append(",")
                  oldSelect1.append("old." + structTypeO(i).name.toUpperCase)
                }
              }
              for (i <- 0 until structTypeI.length) {
                if (i > lengthO - 1) {
                  incSelect1.append(",")
                  incSelect1.append("NULL as " + structTypeI(i).name.toUpperCase)
                  incSelect2.append(",")
                  incSelect2.append("inc." + structTypeI(i).name.toUpperCase)
                } else {
                  if (i == 0) {
                    oldSelect2.append("inc." + structTypeI(i).name.toUpperCase)
                    checkSelect.append("old." + structTypeI(i).name.toUpperCase)
                  } else {
                    oldSelect2.append(",")
                    oldSelect2.append("inc." + structTypeI(i).name.toUpperCase)
                    checkSelect.append(",")
                    checkSelect.append("old." + structTypeI(i).name.toUpperCase)
                  }
                }
              }
              if (oldSelect1.toString != checkSelect.toString) {
                println(DateUtil.getDateTimeNow() + "字段匹配异常");
                println(DateUtil.getDateTimeNow() + "原字段:" + oldSelect1.toString);
                println(DateUtil.getDateTimeNow() + "新字段:" + oldSelect2.toString);
                System.exit(1)
              } else {
                allSelectStr1 = oldSelect1.toString + incSelect1.toString
                allSelectStr2 = oldSelect2.toString + incSelect2.toString
              }
            }
            println(DateUtil.getDateTimeNow() + "allSelectStr: " + allSelectStr1)


            //开始增量处理
            println(DateUtil.getDateTimeNow() + "记录数--增量数据:" + sqlContext.sql("select  * from inc_data ").count())
            println(DateUtil.getDateTimeNow() + "记录数--历史全量:" + sqlContext.sql("select  * from old_data ").count())
            if ("1".equals(isFullImport)) {
              println(DateUtil.getDateTimeNow() + "开始执行merger")
              sqlContext.sql("select  " + allSelectStr1 + " from old_data old left anti join inc_data inc on " + keyrel).createOrReplaceTempView("new_data")
              sqlContext.sql("insert into table new_data select " + allSelectStr2 + " from inc_data inc")
              //sqlContext.sql("select  " + allSelectStr1 + " from old_data old left anti join inc_data inc on " + keyrel).createOrReplaceTempView("Tmp_All_data")
              //sqlContext.sql("select " + allSelectStr2 + " from Tmp_All_data inc ").union(sqlContext.sql("select " + allSelectStr2 + " from inc_data inc")).createOrReplaceTempView("new_data")
            } else if ("2".equals(isFullImport)) {
              println(DateUtil.getDateTimeNow() + "开始执行insert")
              sqlContext.sql("select distinct " + sourceKey + " from inc_data inc").createOrReplaceTempView("Tmp_INC_key")
              sqlContext.sql("select  " + allSelectStr1 + " from old_data old left anti join Tmp_INC_key inc on " + keyrel).createOrReplaceTempView("new_data")
              sqlContext.sql("insert into table new_data select " + allSelectStr2 + " from inc_data inc")
              //sqlContext.sql("select  " + allSelectStr1 + " from old_data old left anti join Tmp_INC_key inc on " + keyrel).createOrReplaceTempView("Tmp_All_data")
              //sqlContext.sql("select " + allSelectStr2 + " from Tmp_All_data inc ").union(sqlContext.sql("select " + allSelectStr2 + " from inc_data inc")).createOrReplaceTempView("new_data")
              //sqlContext.sql("select " + allSelectStr1 + " from old_data old where " + keyrel2).union(sqlContext.sql("select " + allSelectStr2 + " from inc_data inc")).createOrReplaceTempView("new_data")
            } else if ("3".equals(isFullImport)) {
              println(DateUtil.getDateTimeNow() + "开始执行delete")
              sqlContext.sql("select  " + allSelectStr1 + " from old_data old left anti join inc_data inc on " + keyrel).createOrReplaceTempView("new_data")
            } else {
              println(DateUtil.getDateTimeNow() + "未知状态");
              System.exit(1)
            }
            println(DateUtil.getDateTimeNow() + "准备复制数据到中间表")
            if (StringUtils.isEmpty(paritionKey)) {
              //sqlContext.sql("select  * from new_data new").coalesce(collN).write.mode(SaveMode.Overwrite).save(newPathMerged)
              sqlContext.sql("select  * from new_data new").repartition(collN).write.mode(SaveMode.Overwrite).save(newPathMerged)
              //sqlContext.sql("select  * from new_data new").write.mode(SaveMode.Overwrite).save(newPathMerged)
            } else {
              sqlContext.sql("select  * from new_data new").coalesce(collN).write.partitionBy(paritionKey).mode(SaveMode.Overwrite).save(newPathMerged)
            }
            println(DateUtil.getDateTimeNow() + "记录数--结果数据:" + sqlContext.sql("select  * from new_data new").count())
            //更新成功后，修改dbManage的INCR_LAST_VAL
            println(DateUtil.getDateTimeNow() + "数据同步完成，更新状态...")
            var importDate = DateUtil.getDateTimeNow()
            if ("2".equals(isFullImport)) {
              val dbsql = "update DBMANAGE set UPDATE_SQL_INCR_LAST_VAL = '" + importDate + "' where ID='" + DBID + "'"
              jdbcConnection.prepareStatement(dbsql).executeUpdate()
            } else if ("3".equals(isFullImport)) {
              val dbsql = "update DBMANAGE set DEL_SQL_INCR_LAST_VAL = '" + importDate + "' where ID='" + DBID + "'"
              jdbcConnection.prepareStatement(dbsql).executeUpdate()
            } else if ("1".equals(isFullImport)) {
              val dbsql = "update DBMANAGE set INCR_LAST_VAL = '" + importDate + "' where ID='" + DBID + "'"
              jdbcConnection.prepareStatement(dbsql).executeUpdate()
            } else {
              println(DateUtil.getDateTimeNow() + "数据同步完成.")
            }
          }//处理结束
        } else {
          println(DateUtil.getDateTimeNow() + "未找到merged文件夹：" + oldPathMerged)
          println(DateUtil.getDateTimeNow() + "前置抽取任务还未完成，请抽取完数据再次执行合并：" + oldPath)
        }
      } else {
        println(DateUtil.getDateTimeNow() + "mysql中无data source信息，退出合并")
      }
    } catch {
      case fex: FileNotFoundException =>
        //restore(oldPathMerged + "/all")
        throw new FileNotFoundException
      case ex: Exception =>
        ex.printStackTrace()
        //restore(oldPathMerged + "/all")
        throw new Exception(ex)
      case th: Throwable =>
        //restore(oldPathMerged + "/all")
        throw new Exception(th)
    } finally {
      rs.close()
      jdbcConnection.close()
      sqlContext.stop()
    }
  }

  /******************************************************************************************************************/

  /******************************************************************************************************************/

  /******************************************************************************************************************/
  def checkReady(oldPath: String): Boolean = {
    val conf = new Configuration();
    val oldDfsPath = new Path(oldPath);
    val fs = oldDfsPath.getFileSystem(conf)
    val lockPath = new Path(oldPath + "/sqooplock")
    var isReady = false
    if (fs.exists(lockPath)) {
      isReady = false
    } else {
      isReady = true
    }
    return isReady
  }

  /******************************************************************************************************************/

  /******************************************************************************************************************/

  /******************************************************************************************************************/
  def loadData(dirPath: String): DataFrame = {
    val conf = new Configuration();
    val path = new Path(dirPath);
    val fs = path.getFileSystem(conf)
    try {
      if (fs.exists(path) && fs.getContentSummary(path).getFileCount > 1) {
        println(DateUtil.getDateTimeNow() + s"读取文件:$path")
        //sqlContext.read.load(dirPath)
        sqlContext.read.parquet(dirPath)
      } else {
        println(DateUtil.getDateTimeNow() + s"条件不满足未读取文件:$path")
        null
      }
    } catch {
      case ex: Exception => {
        println(DateUtil.getDateTimeNow() + s"出现异常，未读取文件:$path")
        ex.printStackTrace()
        null
      }
    }
  }

  /******************************************************************************************************************/
  def getJDBCConnection(): Connection = {
    try {
      val driverName = "com.mysql.jdbc.Driver"
      val urlString = DataSrcETL.urlString
      val userName = DataSrcETL.userName
      val password = DataSrcETL.password
      println(DateUtil.getDateTimeNow() + urlString);
      println(DateUtil.getDateTimeNow() + userName);
      //println(DateUtil.getDateTimeNow() + password);
      Class.forName(driverName);
      return DriverManager.getConnection(urlString, userName, password);
    } catch {
      case ex: Exception => {
        println(DateUtil.getDateTimeNow() + "获取mysql连接出错")
        ex.printStackTrace()
        return null
      }
    }
  }
}

/******************************************************************************************************************/
/******************************************************************************************************************/
object DataSrcETL {
  private final val p = new Properties()
  private var urlString = ""
  private var userName = ""
  private var password = ""

  /******************************************************************************************************************/
  def main(arg: Array[String]) {
    //var rdbRoot = "/root/data/rdb/";
    if (arg.length !=  4) {
      System.out.println(DateUtil.getDateTimeNow() + "请输入必要参数")
      System.out.println(DateUtil.getDateTimeNow() + arg.length);
      for (i <- 0 until arg.length) {
        System.out.println(DateUtil.getDateTimeNow() + arg(i));
      }
      System.exit(0);
    }
    val cfgPath1 = arg(0)
    val dbID = arg(1)
    val rdbRoot = arg(2)
    val colNum = arg(3)
    //接收多一个参数来指定rdb路径
    //if (arg.length == 2) {
    //  rdbRoot = arg(1)
    //}
    loadProperties(cfgPath1)
    val hdfsPath = p.getProperty("master.url")
    urlString = p.getProperty("mysql.url")
    userName = p.getProperty("mysql.username")
    password = p.getProperty("mysql.password")
    println(DateUtil.getDateTimeNow() + "MASTER_URL: "+ hdfsPath)
    println(DateUtil.getDateTimeNow() + "urlString: "+ urlString + ", username: "+userName)
    System.out.println(DateUtil.getDateTimeNow() + "simple merge启动：请求号为=" + dbID);
    val manager = new DataSrcETL(dbID)
    manager.merge(dbID, rdbRoot, "", hdfsPath,colNum);

  }
  /******************************************************************************************************************/
  def loadProperties(cfgPath:String) {
    //val appStream = getClass()
    //.getClassLoader()
    //.getResourceAsStream("simple-merge.properties")
    //p.load(appStream)
    val appStream = new FileInputStream(cfgPath)
    p.load(appStream)

    //val appStream = new FileInputStream("/home/bigdata/cfg/simple-merge.properties")
    //p.load(appStream)
  }
}
