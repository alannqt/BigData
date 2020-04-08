import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.apache.spark.sql.types._

import org.apache.log4j.Logger
import org.apache.log4j.Level

object IngestWorkLog {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val inputFilePath = "file:/Users/mohameddhameemm/IdeaProjects/bigdata/Data/WorkLog/JobSummary*.txt"
    val conf = new SparkConf().setAppName("WorkLog").setMaster("local[2]")
    val context = new SparkContext(conf)
    val spark = SparkSession.builder().appName("WorkLog").getOrCreate()
    val df = spark.read
      .option("inferschema","true")
      .option("header","true")
      .option("delimiter", "\t")
      .csv(inputFilePath)
    df.show(truncate = false)
    df.printSchema()
    df.write.format("csv").save("./test1")
    println("COUNT --> "+df.count())
    //Lets try to get the unique crane list out from the list
    val df_unique_list = df.select("Location").dropDuplicates()
    df_unique_list.show(truncate = false)
  }
}
