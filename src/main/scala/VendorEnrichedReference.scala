import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import gkFunctions.readSchema

object VendorEnrichedReference {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("VendorEnrichedReference").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    val gkConfig: Config = ConfigFactory.load("application.conf")
    val inputLocation = gkConfig.getString("paths.inputLocation")
    val outputLocation = gkConfig.getString("paths.outputLocation")

    val saleAmoutEnrichedFileSchemaFromFile = gkConfig.getString("schema.saleAmoutEnrichedFileSchema")
    val saleAmoutEnrichedFileSchema = readSchema(saleAmoutEnrichedFileSchemaFromFile)
    val vendorReferenceSchemaFromFile = gkConfig.getString("schema.vendorReferenceSchema")
    val finalVendorReferenceSchema = readSchema(vendorReferenceSchemaFromFile)
    val usdReferenceSchemaFromFile = gkConfig.getString("schema.usdReferenceSchema")
    val finalUsdReferenceSchema = readSchema(usdReferenceSchemaFromFile)
    //Handling dates
    val dateToday = LocalDate.now()
    val dateYesterday = dateToday.minusDays(1)
    //val currentDateSuffix = "_" + dateToday.format(DateTimeFormatter.ofPattern("ddMMyyyy"))
    val currentDateSuffix = "_19072020"
    //val previousDateSuffix = "_" + dateYesterday.format(DateTimeFormatter.ofPattern("ddMMyyyy"))
    val previousDateSuffix = "_18072020"

    //Reading Enriched Sale amount data
    val saleAmountEnrichedDF = spark.read.format("csv").schema(saleAmoutEnrichedFileSchema)
      .option("header", "true").option("sep", "|").load(outputLocation + "Enriched/SaleAmountEnrichment/SaleAmountEnriched" + currentDateSuffix)
    //saleAmountEnrichedDF.show(false)
    saleAmountEnrichedDF.createOrReplaceTempView("saleAmountEnriched")

    // Reading Vendor reference data
    val vendorEnrichedReferenceDF = spark.read.format("csv").schema(finalVendorReferenceSchema)
      .option("header", "true").option("sep", "|").load(inputLocation + "Vendors/")
    //vendorEnrichedReferenceDF.show(false)
    vendorEnrichedReferenceDF.createOrReplaceTempView("vendorEnrichedReference")

    //Reading usd reference data
    val usdReferenceDF = spark.read.format("csv").schema(finalUsdReferenceSchema)
      .option("header", "true").option("sep", "|").load(inputLocation + "USD_Rates/")
    //usdReferenceDF.show(false)
    usdReferenceDF.createOrReplaceTempView("usdReference")

    // usd Enriched Data
    val usdEnrichedDataDF = spark.sql("select s.Sale_ID,s.Product_ID,s.Product_Name," +
      "s.Quantity_Sold,s.Vendor_ID,s.Sale_Date,s.Sale_Currency,s.Sale_Amount,u.Exchange_Rate," +
      "Round(s.Sale_Amount/u.Exchange_Rate,2) as USD_Conversion,u.Currency_Updated_Date " +
      "from saleAmountEnriched s left join usdReference u on s.Sale_Currency = u.Currency_Code " +
      "order by Sale_Amount asc")
    // usdEnrichedDataDF.show(false)
    usdEnrichedDataDF.createOrReplaceTempView("usdEnrichedData")

    val finalUsdEnrichedWithVendorDF = spark.sql("select u.Sale_ID,u.Product_ID,u.Product_Name," +
      "u.Quantity_Sold,u.Vendor_ID,v.Vendor_Name,u.Sale_Date,u.Sale_Currency," +
      "u.Sale_Amount,u.Exchange_Rate,u.USD_Conversion " +
      "from usdEnrichedData u left join vendorEnrichedReference v on u.Vendor_ID = v.Vendor_ID " +
      "order by Sale_Amount")
    //finalUsdEnrichedWithVendorDF.show(false)
    finalUsdEnrichedWithVendorDF.createOrReplaceTempView("finalUsdEnrichedWithVendor")
   // finalUsdEnrichedWithVendorDF.printSchema()

    finalUsdEnrichedWithVendorDF.write.format("csv").option("header", "true").option("sep", "|")
      .mode("overwrite").save(outputLocation + "Enriched/Vendor_USD_Enrichment/Vendor_USD_Enriched" + currentDateSuffix)

    //Mysql Connectivity
    finalUsdEnrichedWithVendorDF.write.format("jdbc")
      .options(Map("url" -> "jdbc:mysql://localhost:3306/GKCStores",
        "driver" -> "com.mysql.jdbc.Driver", "dbtable" -> "salesGKC", "user" -> "root", "password" -> "root"))
      .mode("overwrite")
      .save()
  }
}