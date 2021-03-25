package Details

import Scanner.Stocks
import Utils.{CheckDirectory, DfSaveFile, ReadFile}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


class Overview(date: String = "",
               override val spark: SparkSession,
               period: Int = 20,
               today: Boolean = true
              ) extends Stocks (date, period, today) {

  import spark.implicits._

  var indices: Map[String, String] =
    ReadFile.KeyFileUtil(ReadFile.readFromFileByLine(s"./data/indices/indices_$date.txt"))

  val df_today: DataFrame = get_data(date, spark)
  val df_pre_day: DataFrame = get_data(pre_date, spark)

  val up_total: Int = df_today.filter("up_down == 1").select("code").collect.toList.length
  val down_total: Int = df_today.filter("up_down == -1").select("code").collect.toList.length
  val limit_up_total: Int = df_today.filter("limit_up == true").select("code").collect.toList.length
  val limit_down_total: Int = df_today.filter("limit_down == true").select("code").collect.toList.length
  val limit_up_used_total: Int = df_today.filter("limit_up_used == true").select("code").collect.toList.length
  val limit_up_ctn_total: Int = df_today.filter("limit_up_ctn == true").select("code").collect.toList.length
  val limit_up_1_total: Int = df_today.filter("limit_up_count == 1").select("code").collect.toList.length
  val limit_up_2_total: Int = df_today.filter("limit_up_count == 2").select("code").collect.toList.length
  val limit_up_3_total: Int = df_today.filter("limit_up_count == 3").select("code").collect.toList.length
  val limit_up_more_total: Int = df_today.filter("limit_up_count > 3").select("code").collect.toList.length

  val list_pre_1: Array[Row] = df_pre_day.filter("limit_up_count == 1").select("code").collect
  val list_pre_2: Array[Row] = df_pre_day.filter("limit_up_count == 2").select("code").collect
  val list_pre_3: Array[Row] = df_pre_day.filter("limit_up_count == 3").select("code").collect

  val list_2: Array[Row] = df_today.filter("limit_up_count == 2").select("code").collect
  val list_3: Array[Row] = df_today.filter("limit_up_count == 3").select("code").collect
  val list_m: Array[Row] =  df_today.filter("limit_up_count > 2").select("code").collect

  def calc_hit_rate(list: Array[Row], list_pre: Array[Row]):Any = {
    var counter: Int = 0
    for (i <- list_pre.toList) {
      if (list.toList.contains(i)) counter += 1
    }
    if (list_pre.toList.nonEmpty) counter * 100 / list_pre.toList.length.toFloat else ""
  }

  val up_limit_1_2: String = calc_hit_rate(list_2, list_pre_1).formatted("%.2f").toString
  val up_limit_2_3: String = calc_hit_rate(list_3, list_pre_2).formatted("%.2f").toString
  val up_limit_3_m: String = calc_hit_rate(list_m, list_pre_3).formatted("%.2f").toString

  val df: DataFrame = Seq((
    up_total,
    down_total,
    limit_up_total,
    limit_down_total,
    limit_up_ctn_total,
    limit_up_used_total,
    indices("SH_index"),
    indices("SZ_index"),
    indices("GEB_index"),
    limit_up_1_total,
    limit_up_2_total,
    limit_up_3_total,
    limit_up_more_total,
    up_limit_1_2,
    up_limit_2_3,
    up_limit_3_m
  )).toDF(
    "up",
    "down",
    "limit_up",
    "limit_down",
    "limit_up_ctn",
    "limit_up_used",
    "SH_index",
    "SZ_index",
    "GEB_index",
    "limit_up_1",
    "limit_up_2",
    "limit_up_3",
    "limit_up_more",
    "hit_rate_1_to_2(%)",
    "hit_rate_2_to_3(%)",
    "hit_rate_3_to_4(%)")

  def save(): Unit = {
    df.show()
    CheckDirectory(s"./data/overview/")
    CheckDirectory(s"./data/overview/$date/")
    DfSaveFile(df, s"./data/overview/$date/", s"overview_$date")
  }

}
