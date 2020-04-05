import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object HelloWorld {
  def main(args: Array[String]): Unit = {
    //get data from JSON
    val producerPath = "file:/Users/mohameddhameemm/IdeaProjects/bigdata/producer.json"
    val confx = new SparkConf().setAppName("ProducerData").setMaster("local[2]")
    val contextx = new SparkContext(confx)
    val sparkx = SparkSession.builder.appName("Test").config("x","y").getOrCreate()

    val dfProd = sparkx.read.json(producerPath)
    dfProd.show()
  }
}
