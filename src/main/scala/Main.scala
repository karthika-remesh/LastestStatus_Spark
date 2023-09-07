import org.slf4j.LoggerFactory
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset}
import org.apache.spark.sql.expressions.Window
import scala.io.Source
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.AnalysisException
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JArray
import org.json4s.jackson.JsonMethods.parse
import org.slf4j.Logger
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Row
import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{date_format, from_unixtime, to_timestamp}
import org.apache.hadoop.fs.Path
import org.json4s.JsonAST

case class MyData(cei_code: String, p_key: String)

object Main {

  def main(args: Array[String]): Unit = {

    val logger: Logger = LoggerFactory.getLogger(getClass)
    val spark = SparkSession
      .builder()
      .appName("epoch")
      .getOrCreate()

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    try {
    val inputFilePath = args(0)
    val outputFilePath = args(1)
    val configFile = args(2)
    val epoch = (args(3).toLong)/1000
    import spark.implicits._
    val inputDF = spark.read.option("header", "true").csv(inputFilePath)
    val inputDF2 = inputDF.withColumn(
      "updated_at",
      to_timestamp($"updated_at", "yyyy-MM-dd HH:mm:ss.SSS")
    )
    val filteredDF = inputDF2.withColumn("epoch",lit(epoch)).filter($"updated_at" <= to_timestamp(lit(epoch)))
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val inputStream = fs.open(new Path(configFile))
    val jsonString = Source.fromInputStream(inputStream).mkString
    val jsonArray = parse(jsonString).asInstanceOf[JsonAST.JArray]
    import spark.implicits._
    implicit val formats = DefaultFormats
    val configDataList = jsonArray.arr.map { json =>
      val cei_code = (json \ "cei_code").extract[String]
      val p_key = (json \ "p_key").extract[String]
      MyData(cei_code, p_key)
    }.toList

    configDataList.foreach { myClassObj =>
      val code = myClassObj.cei_code
      val pKey = myClassObj.p_key
      println(s"Code: $code, P_Key: $pKey")
      val winSpec = Window.partitionBy(pKey).orderBy(desc("updated_at"))
      val FinalDF = filteredDF
        .filter($"cei_code" === code)
        .withColumn("row", row_number().over(winSpec))

      val outDF = FinalDF
        .filter($"row" === 1)
        .drop($"row")

      outDF.show()

      outDF.write
        .partitionBy("cei_code", "updated_at")
        .mode("overwrite")
        .csv(outputFilePath)
    }

    } catch {
      case _: AnalysisException => {
        logger.warn("File Not Found")
      }
    }
    spark.stop()
  }
}
