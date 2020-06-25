package EDA

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/*
 * $ sbt package
 * $ $SPARK_HOME/bin/spark-submit --class EDA.EDA --master local[*] target/scala-2.11/ooni_2.11-0.1.jar
 */

// This case class for the Dataset row needs to be outside the object with main() method
case class flatVanillaTorDSRow(
                                platform: String,
                                measurement_start_time: java.sql.Timestamp,
                                country: String,
                                probe_city: String,
                                error: String,
                                success: Boolean,
                                timeout: Long,
                                tor_progress_summary: String,
                                test_runtime: Double,
                                test_start_time: java.sql.Timestamp
                              )

object EDA {
  def main(args: Array[String]): Unit = {

    def createSparkSession(awsAccessKeyId: String, awsSecretAccessKey: String): SparkSession = {
      println("Creating Spark session...")

      val spark: SparkSession = SparkSession
        .builder()
        .appName("OONI EDA")
        .config("fs.s3a.access.key", awsAccessKeyId)
        .config("fs.s3a.secret.key", awsSecretAccessKey)
        .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .master("local[*]") // comment out when running with spark-submit
        .getOrCreate()

      spark.sparkContext.setLogLevel("ERROR")

      spark
    }

    // Get AWS credentials from src/main/resources/aws.conf
    val awsConfig = ConfigFactory.parseFile(new File("src/main/resources/aws.conf"))
    val awsAccessKeyId = awsConfig.getString("aws.awsAccessKeyId")
    val awsSecretAccessKey = awsConfig.getString("aws.awsSecretAccessKey")

    // Get Spark session
    val spark: SparkSession = createSparkSession(awsAccessKeyId, awsSecretAccessKey)

    import spark.implicits._

    // Define endpoints
    val inputEndpoint = "s3a://ooni-data/autoclaved/jsonl/" // "*-ndt-*-0.2.0-probe.json"
    val outputEndpoint = "s3a://ldnicolasmay-bucket/"

    // Define Vanilla Tor data schema
    val jsonSchema = StructType(
      List(
        StructField("annotations", StructType(List(
          StructField("platform", StringType, nullable = true)
        )), nullable = true),
        StructField("backend_version", StringType, nullable = true),
        StructField("bucket_date", DateType, nullable = true),
        StructField("data_format_version", StringType, nullable = true),
        StructField("id", StringType, nullable = true),
        StructField("input", StringType, nullable = true),
        StructField("input_hashes", ArrayType(StringType, containsNull = true), nullable = true),
        StructField("measurement_start_time", TimestampType, nullable = true),
        StructField("options", ArrayType(StringType, containsNull = true), nullable = true),
        StructField("probe_asn", StringType, nullable = true),
        StructField("probe_cc", StringType, nullable = true),
        StructField("probe_city", StringType, nullable = true),
        StructField("probe_ip", StringType, nullable = true),
        StructField("report_filename", StringType, nullable = true),
        StructField("report_id", StringType, nullable = true),
        StructField("software_name", StringType, nullable = true),
        StructField("software_version", StringType, nullable = true),
        StructField("test_keys", StructType(List(
          StructField("error", StringType, nullable = true),
          StructField("success", BooleanType, nullable = true),
          StructField("timeout", LongType, nullable = true),
          StructField("tor_log", StringType, nullable = true),
          StructField("tor_progress", LongType, nullable = true),
          StructField("tor_progress_summary", StringType, nullable = true),
          StructField("tor_progress_tag", StringType, nullable = true),
          StructField("tor_version", StringType, nullable = true),
          StructField("transport_name", StringType, nullable = true)
        )), nullable = true),
        StructField("test_name", StringType, nullable = true),
        StructField("test_runtime", DoubleType, nullable = true),
        StructField("test_start_time", TimestampType, nullable = true),
        StructField("test_version", StringType, nullable = true)
      )
    )

    // Define Vanilla Tor S3 key
    // val vanillaTorProbeJson: String = "2020-05-28/*-vanilla_tor*-0.2.0-probe.json"
    // val vanillaTorProbeJson: String = "2020-05-**/*-vanilla_tor*-0.2.0-probe.json"
    val vanillaTorProbeJson: String = "2020-**-**/*-vanilla_tor*-0.2.0-probe.json"
    val vanillaTorDataEndpoint: String = inputEndpoint + vanillaTorProbeJson

    // Read JSON data
    val vanillaTorDF: DataFrame = spark.read
      .schema(jsonSchema)
      .json(vanillaTorDataEndpoint)
      .cache()
    // vanillaTorDF.printSchema()
    // vanillaTorDF.show(10, truncate = false)
    // println(vanillaTorDF.schema.fields)
    // println(vanillaTorDF.schema.fieldNames)

    val flatVanillaTorDS: Dataset[flatVanillaTorDSRow] = vanillaTorDF
      .select("annotations.platform",
        "measurement_start_time",
        "probe_cc",
        "probe_city",
        "test_keys.error",
        "test_keys.success",
        "test_keys.timeout",
        "test_keys.tor_progress_summary",
        "test_runtime",
        "test_start_time"
      )
      .withColumnRenamed("probe_cc", "country")
      .as[flatVanillaTorDSRow]
      .cache()

    // val myObjEncoder = org.apache.spark.sql.Encoders.kryo[flatVanillaTorDSRow]
    // val flatVanillaTorDS: Dataset[flatVanillaTorDSRow] = flatVanillaTorDF.as[flatVanillaTorDSRow] // (myObjEncoder)
    // flatVanillaTorDS.cache()
    // flatVanillaTorDS.printSchema()
    // flatVanillaTorDS.show()

    val countriesOfInterest = List("DE", "RU", "US")

    val foo: DataFrame = flatVanillaTorDS
      .filter($"country".isInCollection(countriesOfInterest))
      .groupBy('country, 'platform, 'success)
      .count()
      .orderBy(
        asc("country"),
        asc("platform"),
        asc("success")
      )
    foo.show()
    foo.write
      .mode("overwrite")
      .csv(outputEndpoint + "foo.csv")

    val bar: DataFrame = flatVanillaTorDS
      .filter($"country".isInCollection(countriesOfInterest))
      .groupBy('country, 'platform, 'success)
      .agg(avg('test_runtime).as("avg_test_runtime"))
      .orderBy(
        asc("country"),
        asc("platform"),
        asc("success")
      )
    bar.show()
    bar.write
      .mode("overwrite")
      .csv(outputEndpoint + "bar.csv")

    spark.stop()

  }
}
