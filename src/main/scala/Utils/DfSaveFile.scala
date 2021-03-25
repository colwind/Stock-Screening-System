package Utils

import org.apache.spark.sql.DataFrame

object DfSaveFile {

  def apply(df: DataFrame, file_path: String, file_name: String): Unit = {

    df.coalesce(1).write.option("header", "true").csv(s"$file_path$file_name")

//    df.toJavaRDD.coalesce(1).saveAsTextFile(s"$file_path$file_name")

  }

}
