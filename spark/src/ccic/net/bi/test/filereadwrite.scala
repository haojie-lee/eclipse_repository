package ccic.net.bi.test

import scala.io.Source

object filereadwrite {
  def main (args:Array[String]){
    val file = Source.fromFile("d:/123.txt")
    for(line<-file.getLines()){
        println(line)
    }
    file.close();    
  }
}