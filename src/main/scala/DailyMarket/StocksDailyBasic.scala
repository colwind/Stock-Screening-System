package DailyMarket

import Utils.{CheckDirectory, DfSaveFile, ReadCSV}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, regexp_replace}

class StocksDailyBasic(val date: String, val spark: SparkSession) extends DailySourceData {

  val file_name = "daily_info"
  var df_daily_all: DataFrame = get_data(date, spark)

  var df: DataFrame = df_daily_all
  var df_new: DataFrame = df_daily_all.filter("new == true")

  df_daily_all = df_daily_all.filter("new != true")
  df_daily_all = df_daily_all.filter("st != true")
  df_daily_all = df_daily_all.filter("b != true")

  val df_daily_data: DataFrame = df_daily_all.select(
    "code", "name", "enname", "open", "high", "low", "close", "pre_close",
    "change", "pct_chg_exact", "amplitude", "vol", "amount", "sector",
    "limit_up", "limit_down", "limit_up_used", "limit_up_ctn",
    "limit_up_count", "up_ctn", "up_count", "up_true_count",
    "turnover_rate", "volume_ratio", "ma_3", "pct_chg_3",
    "ma_5", "pct_chg_5", "ma_10", "pct_chg_10"
  )

  val df_basic_info: DataFrame = df_daily_all.select(
    "code", "name", "pe", "pe_ttm", "pb", "ps", "ps_ttm",
    "total_share", "float_share", "free_share", "total_mv", "circ_mv",
    "area", "industry", "fullname", "enname", "market", "exchange",
    "curr_type", "list_status", "list_date", "delist_date"
  )

  def save(): Unit = {
    df.show()
    CheckDirectory(s"./data/market/")
    CheckDirectory(s"./data/market/$date/")
    DfSaveFile(df, s"./data/market/$date/", s"$file_name" + "_" + s"$date")
  }

}

trait DailySourceData {

  def get_data(date: String, spark: SparkSession): DataFrame = {

    val daily_path =  s"./data/daily_source/$date.csv"

    var df_daily_all: DataFrame = ReadCSV(spark, daily_path)
    df_daily_all = df_daily_all
      .withColumn("sector", regexp_replace(col("sector"), "sh", "SH"))
    df_daily_all = df_daily_all
      .withColumn("sector", regexp_replace(col("sector"), "sz", "SZ"))
    df_daily_all = df_daily_all
      .withColumn("sector", regexp_replace(col("sector"), "kcb", "STAR"))
    df_daily_all = df_daily_all
      .withColumn("sector", regexp_replace(col("sector"), "cyb", "GEB"))

    df_daily_all
  }
}
