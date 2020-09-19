import java.time.LocalDate

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.typesafe.config._
import gkFunctions.readSchema
import java.time.LocalDate._
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatter._

object DailyDataIngestAndRefine {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DailyDataIngestAndRefine").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val gkConfig: Config = ConfigFactory.load("application.conf")
    val inputLocation = gkConfig.getString("paths.inputLocation")
    val outputLocation = gkConfig.getString("paths.outputLocation")
    val landingFileSchema = gkConfig.getString("schema.landingFileSchema")
    val holdFileSchema = gkConfig.getString("schema.holdFileSchema")
    import spark.implicits._
    // Define schema
    /*val landingFileSchema = StructType(Array(StructField("Sale_ID", StringType, true),
      StructField("Product_ID", StringType, true),
      StructField("Quantity_Sold", IntegerType, true),
      StructField("Vendor_ID", StringType, true),
      StructField("Sale_Date", TimestampType, true),
      StructField("Sale_Amount", FloatType, true),
      StructField("Sale_Currency", StringType, true)))*/

    // define schema through application.conf
    val finalLandingFileSchema = readSchema(landingFileSchema)
    val finalHoldFileSchema = readSchema(holdFileSchema)
    // Handling dates
    val dateToday = LocalDate.now()
    val dateYesterday = dateToday.minusDays(1)
    //val currentDateSuffix = "_" + dateToday.format(DateTimeFormatter.ofPattern("ddMMyyyy"))
    val currentDateSuffix = "_19072020"
    //val previousDateSuffix = "_" + dateYesterday.format(DateTimeFormatter.ofPattern("ddMMyyyy"))
    val previousDateSuffix = "_18072020"

    //Reading Landing Data from Inputs
    val landingFileDF = spark.read.format("csv").schema(finalLandingFileSchema).option("sep", "|")
      .load(inputLocation + "Sales_Landing/SalesDump" + currentDateSuffix)
    // landingFileDF.show(false)
    //landingFileDF.printSchema()
    landingFileDF.createOrReplaceTempView("landingFile")

    //reading previous hold data
    val previousHoldDataDF = spark.read.format("csv").schema(finalHoldFileSchema).option("header", "true")
      .option("sep", "|").load(outputLocation + "Hold/HoldData" + previousDateSuffix)
    //previousHoldDataDF.show(false)
    previousHoldDataDF.createOrReplaceTempView("previousHoldData")

    val refreshedLandingDataDF = spark.sql("SELECT l.Sale_ID,l.Product_ID, " +
      "CASE WHEN l.Quantity_Sold IS NULL THEN p.Quantity_Sold ELSE l.Quantity_Sold END AS Quantity_Sold, " +
      "CASE WHEN l.Vendor_ID IS NULL THEN p.Vendor_ID ELSE l.Vendor_ID END AS Vendor_ID," +
      "l.Sale_Date,l.Sale_Amount,l.Sale_Currency " +
      "FROM landingFile l LEFT JOIN previousHoldData p ON l.Sale_ID = p.Sale_ID")
    //refreshedLandingDataDF.show(false)
    refreshedLandingDataDF.createOrReplaceTempView("refreshedLandingData")
    /*val refreshedLandingDataDF = landingFileDF.join(previousHoldDataDF,Seq("Sale_ID"),"Left")
    refreshedLandingDataDF.show(false)*/

    // filtering validLanding data and Hold Data through spark core api
    val validLandingDataDF = refreshedLandingDataDF.filter(col("Quantity_Sold").isNotNull && col("Vendor_ID").isNotNull)
    //validLandingData.show(false)
    validLandingDataDF.createOrReplaceTempView("validLandingData")


    //filtering validLanding data  through spark sql
    /*val validLandingData = spark.sql("select * from landingFile where Quantity_Sold is not null and Vendor_ID is not null")
    validData.show(false)*/

    //released data from previous hold data
    val releasedDataFromHoldDF = spark.sql("select v.Sale_ID from validLandingData v " +
      "join previousHoldData p on v.Sale_ID = p.Sale_ID ")
    // releasedDataFromHoldDF.show(false)
    releasedDataFromHoldDF.createOrReplaceTempView("releasedData")

    val unreleasedDataFromHoldDF = spark.sql("select * from previousHoldData where Sale_ID not in " +
      "(select Sale_ID from releasedData)")
    // unreleasedDataFromHoldDF.show(false)
    unreleasedDataFromHoldDF.createOrReplaceTempView("unreleasedData")

    val invalidLandingDataDF = refreshedLandingDataDF
      .filter(col("Quantity_sold").isNull || col("Vendor_id").isNull)
      .withColumn("Hold_Reason", when(col("Quantity_Sold").isNull, "Quantity_Sold is missing")
        .otherwise(when(col("Vendor_ID").isNull, "Vendor_ID is missing")))
      .union(unreleasedDataFromHoldDF)
    //invalidLandingDataDF.show(false)

    //filtering invalidLanding data or Hold Data through spark sql
    /*val invalidData = spark.sql("select * from landingFile where Quantity_sold is null or Vendor_ID is null")
    invalidData.show(false)*/

    // Saving valid data and hold data for current date
    validLandingDataDF.write.format("csv").option("header", "true").option("sep", "|").mode("overwrite")
      .save(outputLocation + "Valid/ValidData" + currentDateSuffix)
    invalidLandingDataDF.write.format("csv").option("header", "true").option("sep", "|").mode("overwrite")
      .save(outputLocation + "Hold/HoldData" + currentDateSuffix)
  }
}
