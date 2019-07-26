package ccic.net.bi.sparkproject.etl

import ccic.net.bi.sparkproject.etl.SparkEtlFactory
import net.ccic.sparkprocess.service.SparkEtlModelService

/**
  * Created by FrankSen on 2018/7/2.
  */
object SparkEtlModelServiceImpl extends SparkEtlModelService{

  def sparkEtlModelService(args: Array[String]): Unit ={
    SparkEtlFactory.executeMatch(args)
  }



}
