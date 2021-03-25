package Scanner

import Utils.ReadCSV
import org.apache.spark.sql.{DataFrame, SparkSession}

class PremiumConvertBonds(date: String, spark: SparkSession) extends ScreenerBasic(date, spark) {

  override val file_name = "premium_convert_bonds"

  override val file_path = s"data/screener/$date/$file_name.csv"

  save(ReadCSV(spark, file_path)
    .select(
      "cb_code", "cb_name", "stock_code", "stock_name", "pct_chg(%)",
      "pre_close_price", "open_price", "close_price", "high", "low", "volume"
    ))

}