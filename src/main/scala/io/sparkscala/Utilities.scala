package io.sparkscala

import java.io.File
import java.net.URL

import org.apache.spark.sql.functions.{lit, mean}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.sys.process._

object Utilities {

  def createDataFrame(stationName: String, pathOfYourLocal: String, spark: SparkSession): DataFrame = {

    import spark.implicits._

    val localFilePath = s"$pathOfYourLocal/${stationName}data.txt"

    new URL(s"https://www.metoffice.gov.uk/pub/data/weather/uk/climate/stationdata/${stationName}data.txt") #> new File(localFilePath) !! //debug

    /*Since it is not sure how many lines might the header contain, capturing only the rows that starts with a number, like 2001 (as year is the first column
   * and it is NEVER missing or "---" in the data. Only the 'measures' can be missing or "---").
   */

    val stationRDD1 = spark.sparkContext.textFile(localFilePath).map(_.trim()).filter(line => Character.isDigit(line(0)))

    /* 1. For readability, breaking above line into following.
     * 2. Number of white spaces between columns are inconsistent in the data hence removing those extra spaces, and then as per requirement
     * removing "Provisional", "*" and "#". Then trimming each element of inner arrays because I will be casting them to Int while creating DF.
     */

    val stationRDD2 = stationRDD1.map(_.split("\\s", -1).filter(x => x != "" && x != "Provisional").map(_.replaceAll("[*#]", "")).map(_.trim))

    /* Here's a quick REPL test for above code:s

    "2018   4   12.1     6.4       0*    70.6*   152.2#  Provisional".split("\\s").filter(x => x != "" && x != "Provisional").map(x => x.replaceAll("[*#]", ""))

    */
    /* val stationRDD3 = stationRDD2.filter(array => !array.contains("---")) :=> I tried this for simplicity but it significantly dropped the record count. So not doing
     it. Let's create the dataframe and we will deal with '---' later.
     */

    val stationDF = stationRDD2.map(array => WeatherData(array(0).toInt, array(1).toInt, array(2).toString, array(3).toString, array(4).toString, array(5).toString, array(6).toString)).toDF()
    stationDF.createOrReplaceTempView("station")

    /*Now, handling the '---'(missing) values. I am replacing them with average of that column for that particular month of
    that particular station, and not by average of that entire column.
     */


    val stationAvg = stationDF.groupBy('month).agg(mean('tmax_degC) as "avg_tmax", mean('tmin_degC) as "avg_tmin", mean('af_days) as "avg_af", mean('rain_mm) as "avg_rain", mean('sun_hours) as "avg_sun")
    stationAvg.createOrReplaceTempView("station_avg")

    //Below, Casting is done to be more typesafe(for finding max and min etc. in later stage) as initially tmax_degC, tmin_degC etc were case-classed as Strings.
    val cleanStationData = spark.sql(
      """select station.year,
        |station.month,
        |case when station.tmax_degC <> '---' then cast(station.tmax_degC as decimal(4,1)) else cast(station_avg.avg_tmax as decimal(3,1)) end as tmax,
        |case when station.tmin_degC <> '---' then cast(station.tmin_degC as decimal(4,1)) else cast(station_avg.avg_tmin as decimal(3,1)) end as tmin,
        |case when station.af_days <> '---' then cast(station.af_days as int) else cast(station_avg.avg_af as int) end as af_days,
        |case when station.rain_mm <> '---' then cast(station.rain_mm as decimal(4,1)) else cast(station_avg.avg_rain as decimal(4,1)) end as rain,
        |case when station.sun_hours <> '---' then cast(station.sun_hours as decimal(4,1)) else cast(station_avg.avg_sun as decimal(4,1)) end as sun
        |from station inner join station_avg
        |on station.month = station_avg.month
        |order by year, month""".stripMargin).na.drop() // If while replacement, 'average' itslef is null, drop the row. Insignificant data loss may occur.

    //    println(s"Printing for $stationName")
    //    println(s"Count of $stationName is ${cleanStationData.count}")

    //Return the dataframe of a particular station along with a column stating station name.
    cleanStationData.withColumn("stn_name", lit(stationName))

  }

}
