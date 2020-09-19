import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._
import gkFunctions.readSchema
object EnrichedProductReference {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("EnrichedProductReference").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    val gkConfig:Config = ConfigFactory.load("application.conf")
    val inputLocation = gkConfig.getString("paths.inputLocation")
    val outputLocation = gkConfig.getString("paths.outputLocation")
    val landingFileSchema = gkConfig.getString("schema.landingFileSchema")
    val finalLandingFileSchema = readSchema(landingFileSchema)
    val productReferenceSchemaFromFile = gkConfig.getString("schema.productReferenceSchema")
    val productReferenceSchema = readSchema(productReferenceSchemaFromFile)
    //Handling dates
    val dateToday = LocalDate.now()
    val dateYesterday = dateToday.minusDays(1)
    //val currentDateSuffix = "_" + dateToday.format(DateTimeFormatter.ofPattern("ddMMyyyy"))
    val currentDateSuffix ="_19072020"
    //val previousDateSuffix = "_" + dateYesterday.format(DateTimeFormatter.ofPattern("ddMMyyyy"))
    val previousDateSuffix ="_18072020"

    //Reading Valid Data(ie validLandingDataDF) as it was clean output of first job
    val validDataDF = spark.read.format("csv").schema(finalLandingFileSchema)
      .option("header","true").option("sep","|").load(outputLocation + "Valid/ValidData" + currentDateSuffix)
    //validDataDF.show(false)
    validDataDF.createOrReplaceTempView("validData")

    //Reading product reference file form headquarters
    val productReferenceDF = spark.read.format("csv").schema(productReferenceSchema)
      .option("header","true").option("sep","|").load(inputLocation + "Products/")
    //productReferenceDF.show(false)
    productReferenceDF.createOrReplaceTempView("productReference")

    // creating product Enriched dataframe
    val productEnrichedDF = spark.sql("select v.Sale_ID,v.Product_ID,p.Product_Name," +
      "v.Quantity_Sold,v.Vendor_ID,v.Sale_Date,v.Quantity_Sold * p.Product_Price as Sale_Amount,v.Sale_Currency" +
      " from validData v left join productReference p on v.Product_ID = p.Product_ID  order by Sale_Amount asc")
    //productEnrichedDF.show(false)

    //save productEnrichedDf
    productEnrichedDF.write.format("csv").option("header","true").option("sep","|").mode("overwrite")
      .save(outputLocation+"Enriched/SaleAmountEnrichment/SaleAmountEnriched"+currentDateSuffix)
  }

}
