import DailyMarket.DailyRun
import Details.DetailsRun
import Scanner.SceenerRun
import org.apache.spark.sql.SparkSession

object Main {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Spark SQL basic")
    .config("spark.master", "local")
    .getOrCreate()

  val date = "20191126"

  def main(args: Array[String]): Unit = {
    new DailyRun(date, spark)
    new DetailsRun(date, spark)
    new SceenerRun(date, spark)
  }

}
