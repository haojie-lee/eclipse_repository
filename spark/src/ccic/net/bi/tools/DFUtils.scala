package net.ccic.sparkprocess.data.utils

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  * Created by FrankSen on 2018/8/28.
  */
object DFUtils{

  def findSpaceFields(structType: StructType, row : Row): String = {
    val cols = structType.fieldNames
    val sb = new StringBuilder()

    for (i <- 0 until cols.length) {
      structType.fields(i).dataType match {
        case StringType =>
          val rowss = String.valueOf(row.getAs[String](cols(i)))
          if(!rowss.isEmpty){
            if (rowss.toString.contains("\r") || rowss.contains("\n")){
              sb.append(cols(i))
              sb.append(" ")
            }
          }
        case DoubleType =>
          val rowss = String.valueOf(row.getAs[Double](cols(i)))
          if(!rowss.isEmpty){
            if (rowss.toString.contains("\r") || rowss.toString.contains("\n")){
              sb.append(cols(i))
              sb.append(" ")
            }
          }
        case IntegerType =>
          val rowss = String.valueOf(row.getAs[Int](cols(i)))
          if(!rowss.isEmpty){
            if (rowss.toString.contains("\r") || rowss.toString.contains("\n")){
              sb.append(cols(i))
              sb.append(" ")
            }
          }
        case LongType =>
          val rowss = String.valueOf(row.getAs[Long](cols(i)))
          if(!rowss.isEmpty){
            if (rowss.toString.contains("\r") || rowss.toString.contains("\n")){
              sb.append(cols(i))
              sb.append(" ")
            }
          }
        case _ => throw new Error("not supprot")
      }
    }
    sb.toString
  }
}
