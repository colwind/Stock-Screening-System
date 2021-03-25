package DailyMarket

import org.apache.spark.sql.{SparkSession}

class LimitUpUsed(override val date: String, spark: SparkSession) extends StocksDailyBasic(date, spark) {

  override val file_name: String = "limit_up_used"

  df = df_daily_all.filter("limit_up_used == true")

  df = df.select(
    "code", "name", "enname",
    "pct_chg_exact", "up_count", "up_true_count",
    "open", "high", "low", "close", "pre_close",
    "change", "amplitude", "vol", "amount", "sector",
    "turnover_rate", "volume_ratio", "ma_3", "pct_chg_3",
    "ma_5", "pct_chg_5", "ma_10", "pct_chg_10"
  )

}