package sample

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SanFranciscoFireCalls {

  def sfFireCalls(): Unit = {

    println("Creamos la sesión de SparkSession")
    val spark = SparkSession.builder()
      .master("local")
      .config("spark.some.config.option", "some-value")
      .appName("San Francisco Fire Calls")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("Asignamos la ruta al fichero y creamos el schema que tendrá el dataframe")
    val pathFile = "C:\\Users\\carlos.bocka\\Desktop\\Mi formación\\Proyectos Spark IntellJ\\SparkProject\\sf-fire-calls.csv"
    val fireSchema = StructType(Array(StructField("CallNumber", IntegerType, true),
      StructField("UnitID", StringType, nullable = true),
      StructField("IncidentNumber", IntegerType, true),
      StructField("CallType", StringType, nullable = true),
      StructField("CallDate", StringType, nullable = true),
      StructField("WatchDate", StringType, nullable = true),
      StructField("CallFinalDisposition", StringType, nullable = true),
      StructField("AvailableDtTm", StringType, nullable = true),
      StructField("Address", StringType, nullable = true),
      StructField("City", StringType, nullable = true),
      StructField("Zipcode", IntegerType, nullable = true),
      StructField("Battalion", StringType, nullable = true),
      StructField("StationArea", StringType, nullable = true),
      StructField("Box", StringType, nullable = true),
      StructField("OriginalPriority", StringType, nullable = true),
      StructField("Priority", StringType, nullable = true),
      StructField("FinalPriority", IntegerType, nullable = true),
      StructField("ALSUnit", BooleanType, nullable = true),
      StructField("CallTypeGroup", StringType, nullable = true),
      StructField("NumAlarms", IntegerType, nullable = true),
      StructField("UnitType", StringType, nullable = true),
      StructField("UnitSequenceInCallDispatch", IntegerType, nullable = true),
      StructField("FirePreventionDistrict", StringType, nullable = true),
      StructField("SupervisorDistrict", StringType, nullable = true),
      StructField("Neighborhood", StringType, nullable = true),
      StructField("Location", StringType, nullable = true),
      StructField("RowID", StringType, nullable = true),
      StructField("Delay", FloatType, nullable = true)))

    val fireDF = spark.read.schema(fireSchema).option("header", "true").csv(pathFile)

    fireDF.cache()

    println("Mostramos las primeras filas del dataframe\n\n")
    fireDF.show()

    println("Mostramos el schema del dataframe\n\n")
    fireDF.printSchema()

    println("Creamos un nuevo dataframe a partir del original")
    val fewFireDF = fireDF.select("IncidentNumber", "AvailableDtTm", "CallType").where(col("CallType") =!= "Medical Incident")

    fewFireDF.show(5, truncate = false)

    spark.stop()
  }
}
