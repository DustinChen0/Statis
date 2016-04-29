package cds.test

/**
  * Created by dustinchen on 23/4/2016.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext


object SimpleApp {

  /**
    * 1  VendorID,
    * 2  tpep_pickup_datetime,
    * 3  tpep_dropoff_datetime,
    * 4  passenger_count,
    * 5  trip_distance,
    * 6  pickup_longitude,
    * 7  pickup_latitude,
    * 8  RateCodeID,
    * 9  store_and_fwd_flag,
    * 10 dropoff_longitude,
    * 11 dropoff_latitude,
    * 12 payment_type,
    * 13 fare_amount,
    * 14 extra,
    * 15 mta_tax,
    * 16 tip_amount,
    * 17 tolls_amount,
    * 18 improvement_surcharge,
    * 19 total_amount
    */

  case class TripData(tpep_pickup_datetime: String, trip_distance: Double, Pickup_longitude: Double, Pickup_latitude: Double,
                      Dropoff_longitude: Double, Dropoff_latitude: Double,
                      Passenger_count: Int, Total_amount: Double)

  def main(args: Array[String]) {
    val date = "2015-04"
    val dataFile = "/Users/dustinchen/Documents/APP/Resources/Yellow/yellow_tripdata_" + date + ".csv"
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[3]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    def checkNull(field: String): String = if (field.trim.equals("")) "0" else field.trim

    val DataOfP2 = sc.textFile(dataFile).filter(line => !line.contains("passenger_count"))
      .map(_.replace(",", " , "))
      .map(_.split(","))
      .map(line => TripData(
        line(1).trim, // tpep_pickup_datetime
        checkNull(line(4)).toDouble, // trip_distance
        checkNull(line(5)).toDouble, // Pickup_longitude
        checkNull(line(6)).toDouble, // Pickup_latitude
        checkNull(line(9)).toDouble, // Dropoff_longitude
        checkNull(line(10)).toDouble, // Dropoff_latitude
        checkNull(line(3)).toInt, // Passenger_count
        checkNull(line(18)).toDouble)) // Total_amount
      .toDF()
    DataOfP2.registerTempTable("DataOfP2")

    val avgDistance = sqlContext.sql("select avg(trip_distance) from DataOfP2")
    avgDistance.coalesce(1).write.json("calculate/avgDistance/" + date + ".json")

    //    val one = sqlContext.range(1, 2)
    //    val Revenue = sqlContext.sql("SELECT SUM(Total_amount) AS RevenueSUM, " +
    //      "AVG(Total_amount) AS RevenueAVG, " +
    //      "COUNT(*) AS TripNumber " +
    //      "FROM DataOfP2").join(one)
    //val StreetHailNum = sqlContext.sql("SELECT COUNT(*) AS StreetHailNum FROM DataOfP2 WHERE Trip_type = 1").join(one)
    //val DispatchNum = sqlContext.sql("SELECT COUNT(*) AS DispatchNum FROM DataOfP2 WHERE Trip_type = 2").join(one)
    //val Result = Revenue.join(StreetHailNum, "id").join(DispatchNum, "id")

    //    Revenue.coalesce(1).write.json("/Users/dustinchen/Documents/APP/Resources/Yellow/Output/yellow_" + date + ".json")

    sc.stop()
  }
}
