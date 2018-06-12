package io.sparkscala

import com.typesafe.config.ConfigFactory
import io.sparkscala.Utilities._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._

case class WeatherData(year: Int, month: Int, tmax_degC: String, tmin_degC: String, af_days: String, rain_mm: String, sun_hours: String)

object WeatherData {

  def main(args: Array[String]): Unit = {

    val config = ConfigFactory.load()

    val pathOfYOurLocal = config.getString("path_of_local")
    //println(s"path of local is : $pathOfYOurLocal")

    val stationList: List[String] = config.getStringList("station_list").toList
    //stationList foreach println

    val spark = SparkSession.builder().appName("The Weather Project").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val AllStationsDF = (for (stationName <- stationList) yield createDataFrame(stationName, pathOfYOurLocal, spark)) reduce (_ union _)

    //    println("Printing for All Stations: ")
    //    println(AllStationsDF.count())
    //    AllStationsDF.show

    AllStationsDF.createOrReplaceTempView("all_stations")

    println("Answering (a) --> Rank stations by how long they have been online..")
    spark.sql("""select stn_name, min(year) as year_since_online from all_stations group by stn_name order by year_since_online""").show

    println("Answering (b) - part1 --> Rank stations by rainfall..")
    spark.sql("""select stn_name, avg(rain) as avg_rain from all_stations group by stn_name order by avg_rain desc""").show

    println("Answering (b) - part2 --> Rank stations by sunshine..")
    spark.sql("""select stn_name, avg(sun) as avg_sunshine from all_stations group by stn_name order by avg_sunshine desc""").show

    println("Answering (d) - part1 --> When was the worst rainfall for each station..")

    val window1 = Window.partitionBy('stn_name).orderBy('rain desc)
    val stnWorstRainfall = AllStationsDF.select('stn_name, 'year, 'month, 'rain).withColumn("rank", rank() over window1).filter('rank === 1).drop('rank).drop('rank).show

    println("Answering (d) - part2 --> When was the best sunshine for each station..")

    val window2 = Window.partitionBy('stn_name).orderBy('sun desc)
    val stnBestSunshine = AllStationsDF.select('stn_name, 'year, 'month, 'sun).withColumn("rank", rank() over window2).filter('rank === 1).drop('rank).drop('rank).show()

    println("Answering (e) --> What are the averages for May across all stations, what was the best/worst year..")

    spark.sql(
      """select stn_name, cast(avg(tmax) as decimal(3,1)) as avg_tmax_may,
        |cast(avg(tmin) as decimal(3,1)) as avg_tmin_may,
        |cast(avg((tmin+tmax)/2) as decimal(3,1)) as avg_tmp_may,
        |cast(avg(af_days) as int) as avg_afdays_may,
        |cast(avg(rain) as decimal(4,1)) as avg_rain_may,
        |cast(avg(sun) as decimal(4,1)) as avg_sun_may
        |from all_stations where month = 5 group by stn_name""".stripMargin).show

    println("Answering (c) What patterns exist.. There are many, I am trying to show some approximated facts..\n")

    println()

    spark.sql("""select stn_name, avg((tmin+tmax)/2) as avg_temp from all_stations group by stn_name""").cache().createOrReplaceTempView("avg_tmp_per_station")

    val min_avg_tmp_per_station = spark.sql("select min(avg_temp) from avg_tmp_per_station").first.getDecimal(0)
    val max_avg_tmp_per_station = spark.sql("select max(avg_temp) from avg_tmp_per_station").first.getDecimal(0)

    println(spark.sql(s"""select stn_name, avg_temp from avg_tmp_per_station where avg_temp=$min_avg_tmp_per_station""").map(x => f"${x.getAs[String]("stn_name")} has been the coldest station with average temperature of ${x.getAs[String]("avg_temp")}").collect.mkString(""))

    println()

    println(spark.sql(s"""select stn_name, avg_temp from avg_tmp_per_station where avg_temp=$max_avg_tmp_per_station""").map(x => s"${x.getAs[String]("stn_name")} has been the hottest station with average temperature of ${x.getAs[String]("avg_temp")}").collect.mkString(""))

    println()

    spark.sql("""select year, avg((tmin+tmax)/2) as avg_temp from all_stations group by year""").cache().createOrReplaceTempView("avg_tmp_per_year")

    val min_avg_tmp_per_year = spark.sql("select min(avg_temp) from avg_tmp_per_year").first.getDecimal(0)
    val max_avg_tmp_per_year = spark.sql("select max(avg_temp) from avg_tmp_per_year").first.getDecimal(0)

    println(spark.sql(s"""select year, avg_temp from avg_tmp_per_year where avg_temp=$min_avg_tmp_per_year""").map(x => s"${x.getAs[String]("year")} has been the coldest year for UK so far with average temperature of ${x.getAs[String]("avg_temp")}").collect.mkString(""))

    println()

    println(spark.sql(s"""select year, avg_temp from avg_tmp_per_year where avg_temp=$max_avg_tmp_per_year""").map(x => s"${x.getAs[String]("year")} has been the hottest year for UK so far with average temperature of ${x.getAs[String]("avg_temp")}").collect.mkString(""))

    println()

    println(spark.sql(
      """select a.stn_name as stn, a.year as yr, a. month as mn, a.tmin as tmp
        |from all_stations a inner join (select min(tmin) as min_temp from all_stations)b
        |on a.tmin = b.min_temp""".stripMargin).map(x => x.getAs[String]("stn") + " has experienced coldest of all days among all stations " +
      "in year " + x.getAs[String]("yr") + " ,month " + x.getAs[String]("mn") + " with minimum temperature of " + x.getAs[String]("tmp")).collect().mkString(" also "))

    println()

    println(spark.sql(
      """select a.stn_name as stn, a.year as yr, a. month as mn, a.tmax as tmp
        |from all_stations a inner join (select max(tmax) as max_temp from all_stations)b
        |on a.tmax = b.max_temp""".stripMargin).map(x => x.getAs[String]("stn") + " has experienced hottest of all days among all stations " +
      "in year " + x.getAs[String]("yr") + " ,month " + x.getAs[String]("mn") + " with maximum temperature of " + x.getAs[String]("tmp")).collect().mkString(" also "))


  }

}