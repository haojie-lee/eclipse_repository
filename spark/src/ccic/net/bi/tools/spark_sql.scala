package ccic.net.bi.tools

import org.apache.spark.sql.SparkSession

object spark_sql {
 //外部传入两个变量，分别是需要插入的表名  及  sql语句
  def main(args: Array[String]){
    val table_name=args(0)
    //val table_name="ccic_car.test"
    //val job_name="proc_"+args(1)
    //val sql_yuju="select count(*) from t03_prpcmain"
    val sql_yuju=args(1)
    val spark=SparkSession.builder()
                          .enableHiveSupport()
                          .getOrCreate()
                        
      //spark.table(table_name)
      spark.conf.set("spark.sql.broadcastTimeout", 1200)
     spark.sql("insert overwrite table "+table_name+" "+sql_yuju)
     
  }
}