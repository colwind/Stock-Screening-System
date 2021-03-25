package DailyMarket

import org.apache.spark.sql.SparkSession

class DailyRun(date: String, spark: SparkSession) {

  new StocksDailyBasic(date, spark).save()
  new New(date, spark).save()
  new LimitUp(date, spark).save()
  new LimitDown(date, spark).save()
  new LimitUpContinue(date, spark).save()
  new LimitUpUsed(date, spark).save()
  new STAR(date, spark).save()
  new UpContinue(date, spark).save()

}
