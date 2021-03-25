package Details

import org.apache.spark.sql.SparkSession

class DetailsRun(date: String, spark: SparkSession) {

  new CompanyInformation(date, spark).save()
  new Overview(date, spark).save()

}
