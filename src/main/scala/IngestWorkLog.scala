import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.apache.spark.sql.types._

object IngestWorkLog {
  def main(args: Array[String]): Unit = {
    val inputFilePath = "file:/Users/mohameddhameemm/IdeaProjects/bigdata/Data/WorkLog/JobSummary*.csv"
    val conf = new SparkConf().setAppName("WorkLog").setMaster("local[2]")
    val context = new SparkContext(conf)
    val spark = SparkSession.builder().appName("WorkLog").getOrCreate()
    val df = spark.read
      .option("inferschema","true")
      .option("header","true")
      .csv(inputFilePath)
    df.show()
    df.printSchema()
  }
}
