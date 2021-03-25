package Details

import DailyMarket.StocksDailyBasic
import Utils.{CheckDirectory, DfSaveFile}
import org.apache.spark.sql.{DataFrame, SparkSession}

class CompanyInformation (date: String, spark: SparkSession) extends StocksDailyBasic(date, spark) {

  override def save(): Unit = {
    df_basic_info.show()
    CheckDirectory(s"./data/overview/")
    CheckDirectory(s"./data/company_information/$date/")
    DfSaveFile(df_basic_info, s"./data/company_information/$date/", s"company_information_$date")
  }

}
