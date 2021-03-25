package Scanner

import java.util.Date
import java.text.SimpleDateFormat

import DailyMarket.DailySourceData
import org.apache.spark.sql.{DataFrame, SparkSession}
import Utils.ReadFile


class Stocks(var date: String = "",
             val period: Int = 20,
             var today: Boolean = true
            ) extends StockBase with DailySourceData {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Spark SQL basic")
    .config("spark.master", "local")
    .getOrCreate()
  var df_daily_all: DataFrame = get_data(date, spark)

  // Set today's date
  if (date != "") {
    today = false
    date_now = date
    year_now = date_now.substring(0, 4)
  } else {
    today = true
  }

  val pre_date: String = trade_date(trade_date.indexOf(date) - 1)

  // Print time information
  println("-" * 70)
  println(s"Date: $date_now")
  println("Time: " + new SimpleDateFormat("HH:mm:ss").format(new Date))
  println("-" * 70)

}

abstract class StockBase {

  var start_date: String = "20100104"
  var date_now: String = new SimpleDateFormat("yyyyMMdd").format(new Date)
  var year_now: String = date_now.substring(0, 4)

  val trade_date: Array[String] = for (
    x <- ReadFile.readFromFileByLine(s"./data/trade_calendar/trade_date_$year_now.dat")
  ) yield x.slice(1, 9)

  val stock_name_dict: Map[String, String] =
    ReadFile.KeyFileUtil(ReadFile.readFromFileByLine(s"./data/stocks_names.txt"))

  def stock_code_to_name(code: String): Any = {
    try{
      this.stock_name_dict(code)
    } catch {
      case _: Throwable =>
    }
  }

  def get_sector(code: String): String = {
    if (code.slice(0, 3) == "688") {
      "STAR" // Sci-Tech Innovation Board
    } else if (code(0) == '6') {
      "SH"   // Shanghai
    } else if (code(0) == '0') {
      "SZ"   // Shenzhen
    } else if (code(0) == '3') {
      "GEB"  // Growth Enterprise Board
    } else {
      ""
    }
  }
}

