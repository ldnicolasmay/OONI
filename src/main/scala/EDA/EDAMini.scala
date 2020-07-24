package EDA

import java.io.File
import java.util.Calendar

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.model.{ListObjectsV2Result, S3ObjectSummary}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConversions._
import scala.io.Source

/*
 * $ sbt package
 *
 * # Local Run
 * $ $SPARK_HOME/bin/spark-submit --class EDA.EDA --master local[*] target/scala-2.11/ooni_2.11-0.1.jar
 *
 * # EMR Cluster Run
 * $ $SPARK_HOME/bin/spark-submit \
 *   --class EDA.EDA \
 *   --jars /path/to/com.typesafe_config-1.3.0.jar \
 *   --master yarn
 *   path/to/ooni_2.11-0.1.jar
 */

// This case class for the Dataset row needs to be outside the object with main() method


object EDAMini {
  def main(args: Array[String]): Unit = {

    println(Calendar.getInstance.getTime)

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

    /**
     * CONFIG
     */

    // Get AWS credentials from src/main/resources/aws.conf
    val awsConfig = ConfigFactory.parseFile(new File("src/main/resources/aws.conf"))
    val awsAccessKeyId = awsConfig.getString("aws.awsAccessKeyId")
    val awsSecretAccessKey = awsConfig.getString("aws.awsSecretAccessKey")
    val awsCreds = new BasicAWSCredentials(awsAccessKeyId, awsSecretAccessKey)

    object Foo {
      val awsCreds = new BasicAWSCredentials(awsAccessKeyId, awsSecretAccessKey)
      val s3: AmazonS3 = AmazonS3ClientBuilder.standard()
        .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
        .withRegion(Regions.US_EAST_1)
        .build()
    }

    // Get Spark session
    val spark: SparkSession = createSparkSession(awsAccessKeyId, awsSecretAccessKey)

    val s3: AmazonS3 = AmazonS3ClientBuilder.standard()
      .withRegion(Regions.US_EAST_1)
      .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
      .build()

    val bucket: String = "ooni-data"

    val sc = spark.sparkContext

    val result: ListObjectsV2Result = s3.listObjectsV2(
      bucket,
      "autoclaved/jsonl/2020-05-28/20190430T193926Z-NL-AS42708-web_connectivity-20200528T040104Z_AS42708_r8chEkmU5W8tZqGkj2UPL2GTaLIXIlHMrsuaKDjoSALVDTLwYt-0.2.0-probe.json"
    )

    // val obj_summaries: java.util.List[S3ObjectSummary] = result.getObjectSummaries

    val rdd: RDD[String] = sc.parallelize(result.getObjectSummaries.map(_.getKey).toList)
      .mapPartitions { it =>
        it.flatMap { key =>
          Source.fromInputStream(Foo.s3.getObject(bucket, key).getObjectContent).getLines
        }
      }
    // rdd.foreach(println)

    val df: DataFrame = spark.sqlContext.read.json(rdd.map(identity))
    df.show(numRows = 5, truncate = 40)

    spark.stop()

    println(Calendar.getInstance().getTime)

  }
}