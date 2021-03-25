package Scanner

import Utils.{CheckDirectory, DfSaveFile, ReadCSV}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ScreenerBasic(date: String, spark: SparkSession) {

  val file_name: String = "total"

  val file_path = s"data/screener/$date/$file_name.csv"

  def save(df: DataFrame): Unit = {
    df.show()
    CheckDirectory(s"./data/screener/")
    CheckDirectory(s"./data/screener/$date/")
    DfSaveFile(df, s"./data/screener/$date/", s"$file_name" + "_" + s"$date")
  }

}
