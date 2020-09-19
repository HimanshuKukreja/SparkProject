import org.apache.spark.sql.types._

object gkFunctions {
  def readSchema(s: String):StructType = {
  val a = s.split(" ")
    val fields = a.map(f=> f.split(":")(1) match {
      case "Int" => StructField(f.split(":")(0),IntegerType)
      case "String" => StructField(f.split(":")(0),StringType)
      case "Float" => StructField(f.split(":")(0),FloatType)
      case "Timestamp" => StructField(f.split(":")(0),TimestampType)
    })
    val schema = StructType(fields)
    return schema
  }
}
