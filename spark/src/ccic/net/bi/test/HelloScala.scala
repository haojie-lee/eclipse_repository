package ccic.net.bi.test

object HelloScala {
  
  def main(args:Array[String])={
    val files =(new java.io.File(".")).listFiles()
    for(file<-files)
      println(file)
//    for(i<-1 to 10)
//      println(i)
   // var file=if(!args.isEmpty) args(0) else "scala.txt"
//      var file="scala.txt"
//      if(!args.isEmpty)
//        file=args(0)
     // println(file)
    //println("hello world scala!")
    //dowhile
   // println(looper(100,298))
  //  for(arg<-args) 
   //   println(arg)  
  }
  def dowhile()={
    var line=""
    do{
    line=readLine()
    println("read："+line)
    }
    while(line!="")
  }
  def looper(x:Long,y:Long):Long={
    var a=x
    var b=y
    while(a!=0){
      var temp=a
      a=b%a
      b=temp      
    }
     b
  }
  

  
  
}