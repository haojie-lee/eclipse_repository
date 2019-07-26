package ccic.net.bi.spark.etl

import ccic.net.bi.sparkproject.etl.SparkEtlModelServiceImpl



/**
  * Created by FrankSen on 2018/7/24.
  */
class SparkEtl(args: Array[String]) {


  def executeSparkEtlService(): Unit ={
    SparkEtlModelServiceImpl.sparkEtlModelService(args)
  }

}

/**
  * SparkSql enter
  * param list:
  *  arg(0) -> master.url
  *  arg(1) -> mode
  *  arg(2) -> sql
  *  arg(3) -> tbl_name
  */
object SparkEtl{
  def main(args: Array[String]): Unit = {
    val  sparkEtlCli = new SparkEtl(args)
    sparkEtlCli.executeSparkEtlService()
  }
}
