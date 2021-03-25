package Scanner

import Utils.ReadCSV
import org.apache.spark.sql.{DataFrame, SparkSession}

class LongUpperShadow(date: String, spark: SparkSession) extends ScreenerBasic(date, spark) {

  override val file_name = "long_upper_shadow"

  override val file_path = s"data/screener/$date/$file_name.csv"

  save(ReadCSV(spark, file_path)
    .select(
      "code", "name", "pct_highest(%)", "pct_chg(%)", "pre_close_price",
      "open_price", "close_price", "high", "low", "volume"
    ))

}
