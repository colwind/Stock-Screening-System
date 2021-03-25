package Scanner

import org.apache.spark.sql.SparkSession

class SceenerRun(date: String, spark: SparkSession) {

//  val proc1: Process = Runtime.getRuntime.exec(s"python src/main/python/strategies.py -d $date")
//  proc1.waitFor()

  new MaUps(date, spark)
  new HighVolumeOpen(date, spark)
  new LongUpperShadow(date, spark)
  new TrendUpperShadow(date, spark)
  new PremiumConvertBonds(date, spark)

}
