package Utils
import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadCSV extends App {

  def apply(spark: SparkSession, path: String): DataFrame =
    spark.read.format("csv").option("header", "true").load(path)

}
