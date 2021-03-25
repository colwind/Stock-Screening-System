package Scanner

import Utils.ReadCSV
import org.apache.spark.sql.{DataFrame, SparkSession}

class HighVolumeOpen(date: String, spark: SparkSession) extends ScreenerBasic(date, spark) {

  override val file_name = "high_volume_open"

  override val file_path = s"data/screener/$date/$file_name.csv"

  save(ReadCSV(spark, file_path)
    .select(
      "code", "name", "pct_vol(%)", "volume", "pct_chg(%)",
      "pre_close_price", "open_price", "close_price", "high", "low"
    ))

}
