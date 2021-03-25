package DailyMarket

import org.apache.spark.sql.{DataFrame, SparkSession}

class LimitUp(override val date: String, spark: SparkSession) extends StocksDailyBasic(date, spark) {

  override val file_name: String = "limit_up"

  df = df_daily_all.filter("limit_up == true")

  df = df.select(
    "code", "name", "enname",
    "pct_chg_exact", "open", "high", "low", "close", "pre_close",
    "change", "amplitude", "vol", "amount", "sector",
    "limit_up_count", "up_count", "up_true_count",
    "turnover_rate", "volume_ratio", "ma_3", "pct_chg_3",
    "ma_5", "pct_chg_5", "ma_10", "pct_chg_10"
  )

}
