//package ca

import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.implicits._
import org.apache.spark.sql.SparkSession

object DataProcessor {
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CA Data Processing").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //readRaw(sc)
    testSession(conf)
  }

  def testSession(conf: SparkConf): Unit = {
    val sparkSess =
      SparkSession.builder().appName("SparkSessionZipsExample")
        .config(conf).getOrCreate()

    val df = sparkSess.read.option("header",
      "true").csv("hdfs://localhost:8020/user/cloudera/ca/data/sensor/20-04-07/events-.1586268501681")
    df.show()
  }
  
  def readRaw(ctx: SparkContext) {
    val file = ctx.textFile("hdfs://localhost:8020/user/cloudera/ca/data/sensor/20-04-07/events-.1586268501681")
    //val dataFrame = file.toDF()
//    val hdfs = FileSystem.get(new URI("hdfs://quickstart.cloudera/cloudera/ca/data/20-04-01/"), new Configuration()) 
//    val path = new Path("/path/to/file/")
//    val stream = hdfs.open(path)
//    def readLines = Stream.cons(stream.readLine, Stream.continually( stream.readLine))
    
    //This example checks line for null and prints every existing line consequentally
    //readLines.takeWhile(_ != null).foreach(line => println(line))
    
     file.flatMap(line => line.split(",")).map(_.trim).foreach(printUtf8)
  }
  
  def printUtf8(str: String) {
    val result = new String(str.getBytes(), "UTF-8")
    print(result + ' ')
  }
}