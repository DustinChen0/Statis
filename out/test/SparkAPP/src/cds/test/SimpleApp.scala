package cds.test

/**
  * Created by dustinchen on 23/4/2016.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext


object SimpleApp {

  case class TripData(lpep_pickup_datetime: String, Pickup_longitude: Double, Pickup_latitude: Double,
                      Dropoff_longitude: Double, Dropoff_latitude: Double,
                      Passenger_count: Int, Total_amount: Double, Trip_type: Int)

  def main(args: Array[String]) {
//    val date = "2015-12"
//    val dataFile = "/Users/dustinchen/Documents/APP/Resources/Green/green_tripdata_" + date + ".csv"
val dataFile = "/taxidata/green/green_tripdata*"
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[3]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    def checkNull(field: String): String = if (field.trim.equals("")) "0" else field.trim

    val DataOfP1 = sc.textFile(dataFile).filter(line => !line.contains("Lpep_dropoff_datetime"))
      .map(_.replace(",", " , "))
      .map(_.split(","))
      .map(line => TripData(line(1), checkNull(line(5)).toDouble, checkNull(line(6)).toDouble, checkNull(line(7)).toDouble, checkNull(line(8)).toDouble, checkNull(line(9)).toInt, checkNull(line(18)).toDouble, checkNull(line(20)).toInt))
      .toDF()
    DataOfP1.registerTempTable("DataOfP1")

    val one = sqlContext.range(1, 2)
    val Revenue = sqlContext.sql("SELECT SUM(Total_amount) AS RevenueSUM, " +
      "AVG(Total_amount) AS RevenueAVG, " +
      "COUNT(*) AS TripNumber " +
      "FROM DataOfP1").join(one)
    val StreetHailNum = sqlContext.sql("SELECT COUNT(*) AS StreetHailNum FROM DataOfP1 WHERE Trip_type = 1").join(one)
    val DispatchNum = sqlContext.sql("SELECT COUNT(*) AS DispatchNum FROM DataOfP1 WHERE Trip_type = 2").join(one)
    val Result = Revenue.join(StreetHailNum, "id").join(DispatchNum, "id")

//    Result.coalesce(1).write.json("/Users/dustinchen/Documents/APP/Resources/Green/Output/green_" + date + ".json")
    Result.coalesce(1).write.json("/dschen/ProccessGreen")

    sc.stop()
  }
}
