//package ca

import java.text.ParseException

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Try}

object DataProcessor {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CA Data Processing").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //readRaw(conf)
    //readRaw(sc, conf)
    readRawToDf(sc, conf)
  }

  //  def readRaw(conf: SparkConf): Unit = {
  ////    val sparkSess =
  ////      SparkSession.builder().appName("SparkSessionZipsExample")
  ////        .config(conf).getOrCreate()
  //
  //    val spark = SparkSession.builder().appName("Sensors").config(conf).getOrCreate()
  //    val df = spark.read
  //      .option("inferSchema","true")
  //      .option("header","true")
  //      .option("delimiter",",")
  //      //.option("comment","*")
  //      //.option("encoding", "UTF-8")
  //      .csv("hdfs://localhost:8020/user/cloudera/ca/data/sensor/20-04-08/*")
  //    df.show()
  //    df.printSchema()
  //
  //    //val df = sparkSess.read.option("header",
  //    //  "true").csv("hdfs://localhost:8020/user/cloudera/ca/data/sensor/20-04-07/events-.1586268501681")
  //    //df.show()
  //  }
  //
  //  def readRaw(ctx: SparkContext, conf: SparkConf) {
  //
  //    //read the data as rdd and split the lines
  //    val file = ctx.textFile("hdfs://localhost:8020/user/cloudera/ca/data/sensor/20-04-08/*").map(_.split(",", -1))
  //
  //    //getting the max length from data and creating the schema
  //    val maxlength = file.map(x => (x, x.length)).map(_._2).max
  //    val schema = StructType((1 to maxlength).map(x => StructField(s"col_${x}", StringType, true)))
  //
  //    val sqlContext = SparkSession.builder().appName("Sensors").config(conf).getOrCreate() //new org.apache.spark.sql.SQLContext(ctx)
  //    import sqlContext.implicits._
  //    //parsing the data with the maxlength and populating null where no data and using the schema to form dataframe
  //    val rawdf = sqlContext.createDataFrame(file.map(x => Row.fromSeq((0 to maxlength-1).map(index => Try(x(index)).getOrElse("null")))), schema)
  //
  //    rawdf.show(false)
  //  }

  //source: https://stackoverflow.com/questions/51962274/spark-add-column-to-dataframe-when-reading-csv
  def readRawToDf(ctx: SparkContext, conf: SparkConf) {

    //read the data as rdd and split the lines
    val file: RDD[Array[String]] = ctx.textFile("hdfs://localhost:8020/user/cloudera/ca/data/sensor/20-04-08/*").map(_.split(",", -1))

    val header = file.first()
    val schema = StructType(header.map(fieldName => StructField(fieldName,StringType, true)))

    //getting the max length from data and creating the schema
    val maxlength = file.map(x => (x, x.length)).map(_._2).max
    //val schema = StructType((1 to maxlength).map(x => StructField(s"col_${x}", StringType, true)))

    val sqlContext = SparkSession.builder().appName("Sensors").config(conf).getOrCreate() //new org.apache.spark.sql.SQLContext(ctx)

    //parsing the data with the maxlength and populating null where no data and using the schema to form dataframe
    val rawDf = sqlContext.createDataFrame(file.filter(f => isDate(f(0))).map(x => Row.fromSeq((0 to maxlength-1).map(index => Try(x(index)).getOrElse("null")))
    ), schema)

    rawDf.show(false)
  }

  def isDate(dateStr: String): Boolean = {
    import java.text.SimpleDateFormat
    val sdf = new SimpleDateFormat("dd/MM/yyyy")
    sdf.setLenient(false)

    try {
      sdf.parse(dateStr)
      return true
    }
    catch {
      case e: ParseException =>
        return false
    }
  }

  def printUtf8(str: String) {
    val result = new String(str.getBytes(), "UTF-8")
    println(result)
  }
}