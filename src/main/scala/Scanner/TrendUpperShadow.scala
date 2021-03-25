package Scanner

import Utils.ReadCSV
import org.apache.spark.sql.{DataFrame, SparkSession}

class TrendUpperShadow(date: String, spark: SparkSession) extends ScreenerBasic(date, spark) {

  override val file_name = "trend_upper_shadow"

  override val file_path = s"data/screener/$date/$file_name.csv"

  save(ReadCSV(spark, file_path)
    .select(
      "code", "name", "pct_close(%)", "amplitude", "pct_high(%)", "upper_shadow(%)",
      "close_price", "open_price", "high", "low", "pre_close_price", "amount(E)", "circ_mv(E)"
    ))

}