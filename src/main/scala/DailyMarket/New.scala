package DailyMarket

import org.apache.spark.sql.SparkSession

class New(override val date: String, spark: SparkSession) extends StocksDailyBasic(date, spark) {

  override val file_name: String = "new"

  df = df_new

  df = df.select(
    "code", "name", "enname", "pct_chg_exact",
    "open", "high", "low", "close", "pre_close",
    "change", "amplitude", "vol", "amount", "sector",
    "limit_up", "limit_down", "limit_up_used", "limit_up_ctn",
    "limit_up_count", "up_ctn", "up_count", "up_true_count",
    "turnover_rate", "volume_ratio", "ma_3", "pct_chg_3",
    "ma_5", "pct_chg_5", "ma_10", "pct_chg_10"
  )

}
