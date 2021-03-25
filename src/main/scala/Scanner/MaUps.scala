package Scanner

import Utils.ReadCSV
import org.apache.spark.sql.{DataFrame, SparkSession}

class MaUps(date: String, spark: SparkSession) extends ScreenerBasic(date, spark) {

  override val file_name = "ma_up"

  override val file_path = s"data/screener/$date/$file_name.csv"

  save(ReadCSV(spark, file_path)
    .select(
    "code", "name", "ma_up_count", "pct_chg(%)",
    "MA2", "pct_chg_2(%)", "MA3", "pct_chg_3(%)",
    "MA5", "pct_chg_5(%)", "MA10", "pct_chg_10(%)",
    "MA20", "pct_chg_20(%)", "MA30", "pct_chg_30(%)",
    "pre_close_price", "open_price", "close_price", "high", "low", "volume"
  ))

}
