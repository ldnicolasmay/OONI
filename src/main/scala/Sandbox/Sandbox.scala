package EDA

import java.io.File
import java.util.Calendar

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.model.{ListObjectsV2Request, ListObjectsV2Result}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.JavaConversions._
import scala.io.Source

/*
 * $ sbt package
 *
 * # Local Run
 * $ $SPARK_HOME/bin/spark-submit --class EDA.EDA --master local[*] target/scala-2.11/ooni_2.11-0.1.jar
 *
 * # EMR Cluster Run #1
 * $ $SPARK_HOME/bin/spark-submit \
 *   --class EDA.EDA \
 *   --jars /path/to/com.typesafe_config-1.3.0.jar \
 *   --master yarn
 *   path/to/ooni_2.11-0.1.jar
 *
 * # EMR Cluster Run #2
 * spark-submit \
 *   --class EDA.EDA \
 *   --jars /home/hadoop/config-1.3.0.jar,/home/hadoop/aws-*.jar \
 *   --master yarn \
 *   /home/hadoop/ooni_2.11-0.1.jar
 */

// This case class for the Dataset row needs to be outside the object with main() method
case class FlatVanillaTorDSRow(
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

case class OONIBaseRow(
                        id: String,
                        bucket_date: java.sql.Date,
                        probe_cc: String,
                        test_name: String
                      )

object EDA {
  def main(args: Array[String]): Unit = {

    println(Calendar.getInstance.getTime.toString)

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

    def getOoniEndpoint(inputBucket: String, keyword: String): String = {
      // Define date and datetime range
      val dateRange: String = "2020-05-28"
      val datetimeRange: String = "20200528T????"
      val jsonPathTemplate: String = "autoclaved/jsonl/%1$s/%2$s*-%3$s*-0.2.0-probe.json"
      //val jsonPathTemplate: String = "autoclaved/jsonl/%1$s/*-%3$s*-0.2.0-probe.json"
      val endpointProbeKey: String = jsonPathTemplate.format(dateRange, datetimeRange, keyword)
      inputBucket + endpointProbeKey
    }

    /**
     * CONFIG
     */

    // Get AWS credentials from src/main/resources/aws.conf
    val awsConfig = ConfigFactory.parseFile(new File("src/main/resources/aws.conf"))
    val awsAccessKeyId = awsConfig.getString("aws.awsAccessKeyId")
    val awsSecretAccessKey = awsConfig.getString("aws.awsSecretAccessKey")
    // val awsCreds = new BasicAWSCredentials(awsAccessKeyId, awsSecretAccessKey)

    object PersistentS3ClientObject {
      val awsCreds = new BasicAWSCredentials(awsAccessKeyId, awsSecretAccessKey)
      val s3: AmazonS3 = AmazonS3ClientBuilder.standard()
        .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
        .withRegion(Regions.US_EAST_1)
        .build()
    }

    // Get Spark session
    val spark: SparkSession = createSparkSession(awsAccessKeyId, awsSecretAccessKey)
    import spark.implicits._


    /**
     * SETUP
     */

    val jsonSchemaBase = StructType(List(
      StructField("id", StringType, nullable = true),
      StructField("bucket_date", DateType, nullable = true),
      StructField("probe_cc", StringType, nullable = true),
      StructField("test_name", StringType, nullable = true)
    ))

    // Define Vanilla Tor data schema
    val jsonSchemaVanillaTor = StructType(
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

    // Define endpoints
    val inputBucket = "s3a://ooni-data/"
    val outputBucket = "s3a://ldnicolasmay-bucket/"

    // Define Meek Fronted Requests S3 key
    val meekFrontedRequestsKeyword: String = "meek_fronted_requests"
    val meekFrontedRequestsDataEndpoint: String = getOoniEndpoint(inputBucket, meekFrontedRequestsKeyword)

    // Define Tor S3 key
    val torKeyword: String = "tor"
    val torDataEndpoint: String = getOoniEndpoint(inputBucket, torKeyword)

    // Define Bridge Reachability S3 key
    val bridgeReachabilityKeyword: String = "bridge_reachability"
    val bridgeReachabilityEndpoint: String = getOoniEndpoint(inputBucket, bridgeReachabilityKeyword)

    // Define Vanilla Tor S3 key
    val vanillaTorKeyword: String = "vanilla_tor"
    val vanillaTorDataEndpoint: String = getOoniEndpoint(inputBucket, vanillaTorKeyword)


    /**
     * EXTRACT
     */

    // Read JSON data

    //    val meekFrontedRequestsDF: DataFrame = spark.read
    //      .json(meekFrontedRequestsDataEndpoint)
    //      .cache()
    //    meekFrontedRequestsDF.printSchema()
    //    meekFrontedRequestsDF.show(2, truncate = false)
    //    println(s"Meek Fronted Requests row count: ${meekFrontedRequestsDF.count()}\n\n")

    //    val torDF: DataFrame = spark.read
    //      .json(torDataEndpoint)
    //      .cache()
    //    torDF.printSchema()
    //    torDF.show(2, truncate = false)
    //    println(s"Tor row count: ${torDF.count()}\n\n")

    //    val bridgeReachabilityDF: DataFrame = spark.read
    //      .json(bridgeReachabilityEndpoint)
    //      .cache()
    //    bridgeReachabilityDF.printSchema()
    //    bridgeReachabilityDF.show(2, truncate = false)
    //    println(s"Bridge Reachability row count: ${bridgeReachabilityDF.count()}")

    //    val vanillaTorDF: DataFrame = spark.read
    //      .schema(jsonSchemaVanillaTor)
    //      .json(vanillaTorDataEndpoint)
    //      .cache()
    //    vanillaTorDF.printSchema()
    //    vanillaTorDF.show(2, truncate = false)
    //    // println(vanillaTorDF.schema.fields.toList)
    //    // println(vanillaTorDF.schema.fieldNames.toString)
    //    println(s"Vanilla Tor row count: ${vanillaTorDF.count()}\n\n")


    /**
     * TRANSFORM
     */

    //    val flatVanillaTorDS: Dataset[FlatVanillaTorDSRow] = vanillaTorDF
    //      .select("annotations.platform",
    //        "measurement_start_time",
    //        "probe_cc",
    //        "probe_city",
    //        "test_keys.error",
    //        "test_keys.success",
    //        "test_keys.timeout",
    //        "test_keys.tor_progress_summary",
    //        "test_runtime",
    //        "test_start_time"
    //      )
    //      .withColumnRenamed("probe_cc", "country")
    //      .as[FlatVanillaTorDSRow]
    //      .cache()
    //
    //    // val countriesOfInterest = List("DE", "RU", "US")
    //    val vanillaTorCountryPlatformSuccessCount: DataFrame = flatVanillaTorDS
    //      // .filter($"country".isInCollection(countriesOfInterest))
    //      .groupBy('country, 'platform, 'success)
    //      .count()
    //      .orderBy(
    //        asc("country"),
    //        asc("platform"),
    //        asc("success")
    //      )
    //      .cache()
    //
    //    val vanillaTorCountryPlatformSuccessAvgTestRuntime: DataFrame = flatVanillaTorDS
    //      // .filter($"country".isInCollection(countriesOfInterest))
    //      .groupBy('country, 'platform, 'success)
    //      .agg(avg('test_runtime).as("avg_test_runtime"))
    //      .orderBy(
    //        asc("country"),
    //        asc("platform"),
    //        asc("success")
    //      )
    //      .cache()


    /**
     * LOAD
     */

    //    vanillaTorCountryPlatformSuccessCount.show()
    //    vanillaTorCountryPlatformSuccessCount.write
    //      .mode("overwrite")
    //      .csv(outputBucket + "vanillaTorCountryPlatformSuccessCount.csv")
    //
    //    vanillaTorCountryPlatformSuccessAvgTestRuntime.show()
    //    vanillaTorCountryPlatformSuccessAvgTestRuntime.write
    //      .mode("overwrite")
    //      .csv(outputBucket + "vanillaTorCountryPlatformSuccessAvgTestRuntime.csv")

    val sc = spark.sparkContext

    //    val request = new ListObjectsV2Request()
    //    request.setBucketName("ooni-data")
    //    request.setPrefix("autoclaved/jsonl/2020-05-28/")
    //    request.setMaxKeys(1000)
    // def s3 = new AmazonS3Client(new auth.BasicAWSCredentials(awsAccessKeyId, awsSecretAccessKey))
    // def s3 = new AmazonS3ClientBuilder(new auth.BasicAWSCredentials(awsAccessKeyId, awsSecretAccessKey))
    // val objs = s3.listObjects(request)
    // println(objs)
    //    val awsCreds: BasicAWSCredentials = new BasicAWSCredentials(awsAccessKeyId, awsSecretAccessKey)
    //    val s3: AmazonS3 = AmazonS3ClientBuilder.standard()
    //      .withRegion(Regions.US_EAST_1)
    //      .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
    //      .build()

    // println(s3.listBuckets().toArray().toList)
    // for { bucket <- s3.listBuckets().toArray.toList } println(bucket)

    //    // 2014-09-02
    //    // 2020-05-28
    //    val result: ListObjectsV2Result = s3.listObjectsV2(
    //      "ooni-data",
    //      // "autoclaved/jsonl/2020-05-28/20190430T193926Z-NL-AS42708-web_connectivity-20200528T040104Z_AS42708_r8chEkmU5W8tZqGkj2UPL2GTaLIXIlHMrsuaKDjoSALVDTLwYt-0.2.0-probe.json"
    //      "autoclaved/jsonl/2020-05-31/"
    //    )
    //    val obj_summaries: java.util.List[S3ObjectSummary] = result.getObjectSummaries
    //    val bucket: String = "ooni-data"
    //
    //    // result.getObjectSummaries.foreach(println)

    //    def keys(bucket: String, prefix: String) =
    //      nextBatch(PersistentS3ClientObject.s3.listObjects(bucket, prefix))
    //
    //    @scala.annotation.tailrec
    //    def nextBatch(listing: ObjectListing, keys: List[String] = Nil): List[String] = {
    //      val pageKeys = listing.getObjectSummaries.map(_.getKey).toList
    //      if (listing.isTruncated)
    //        nextBatch(PersistentS3ClientObject.s3.listNextBatchOfObjects(listing), pageKeys ::: keys)
    //      else
    //        pageKeys ::: keys
    //    }

    // val clientRegion = Regions.DEFAULT_REGION
    val clientRegion = Regions.US_EAST_1
    val bucketName = "ooni-data"
    val prefix = "autoclaved/jsonl/2020-"
    //    var res: ListObjectsV2Result = null
    //    var result: List[String] = List()

    //    try {
    //      val s3Client = AmazonS3ClientBuilder.standard
    //        // .withCredentials(new ProfileCredentialsProvider)
    //        .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
    //        .withRegion(clientRegion)
    //        .build
    //      // System.out.println("Listing objects")
    //      // maxKeys is set to 2 to demonstrate the use of ListObjectsV2Result.getNextContinuationToken()
    //      val req = new ListObjectsV2Request()
    //        .withBucketName(bucketName)
    //        .withPrefix("autoclaved/jsonl/2020-05-3")
    //        // .withMaxKeys(1000)
    //      // var result: ListObjectsV2Result = null
    //      do {
    //        res = s3Client.listObjectsV2(req)
    //        result ++= res.getObjectSummaries.map(_.getKey).toList
    //        //        for (objectSummary <- result.getObjectSummaries) {
    //        //          System.out.printf(" - %s (size: %d)\n", objectSummary.getKey, objectSummary.getSize)
    //        //        }
    //        // If there are more than maxKeys keys in the bucket, get a continuation token and list the next objects.
    //        val token = res.getNextContinuationToken
    //        System.out.println("Next Continuation Token: " + token)
    //        req.setContinuationToken(token)
    //      } while ( {
    //        res.isTruncated
    //      })
    //    } catch {
    //      case e: AmazonServiceException =>
    //        // The call was transmitted successfully, but Amazon S3 couldn't process it, so it returned an error response.
    //        e.printStackTrace()
    //      case e: SdkClientException =>
    //        // Amazon S3 couldn't be contacted for a response, or the client couldn't parse the response from Amazon S3.
    //        e.printStackTrace()
    //    }

    //    try {
    //      val s3Client = PersistentS3ClientObject.s3
    //      val req: ListObjectsV2Request = new ListObjectsV2Request()
    //        .withBucketName(bucketName)
    //        .withPrefix("autoclaved/jsonl/2020-05-31/")
    //      val res: ListObjectsV2Result = PersistentS3ClientObject.s3.listObjectsV2(req)
    //      nextBatch(res)
    //    }

    //    def keys(bucket: String, prefix: String) =
    //      nextBatch(PersistentS3ClientObject.s3.listObjects(bucket, prefix))
    //
    //    @scala.annotation.tailrec
    //    def nextBatch(listing: ObjectListing, keys: List[String] = Nil): List[String] = {
    //      val pageKeys = listing.getObjectSummaries.map(_.getKey).toList
    //      if (listing.isTruncated)
    //        nextBatch(PersistentS3ClientObject.s3.listNextBatchOfObjects(listing), pageKeys ::: keys)
    //      else
    //        pageKeys ::: keys
    //    }

    val testNamesOfInterest = List("http_requests", "meek_fronted_requests_test", "vanilla_tor", "psiphon", "tor")
    val keyFilterRegex: String = ".*(" + testNamesOfInterest.mkString("|") + ").*"
    val filterKey = (x: String) => x.matches(keyFilterRegex)

    @scala.annotation.tailrec
    def nextBatch(req: ListObjectsV2Request, res: ListObjectsV2Result, keys: List[String] = Nil): List[String] = {
      val pageKeys = res.getObjectSummaries.map(_.getKey).toList.filter(filterKey)
      if (res.isTruncated) {
        val nextToken: String = res.getNextContinuationToken
        //println("Next Continuation Token: " + nextToken)
        req.setContinuationToken(nextToken)
        nextBatch(req, PersistentS3ClientObject.s3.listObjectsV2(req), pageKeys ::: keys)
      } else {
        pageKeys ::: keys
      }
    }

    def getPaginatedS3Keys(bucketName: String, prefix: String): List[String] = {
      val req: ListObjectsV2Request = new ListObjectsV2Request()
        .withBucketName(bucketName)
        .withPrefix(prefix)
      val res: ListObjectsV2Result = PersistentS3ClientObject.s3.listObjectsV2(req)
      nextBatch(req, res)
    }

    //    val s3Keys: List[String] = getPaginatedS3Keys(bucketName, prefix)
    //    //    val req: ListObjectsV2Request = new ListObjectsV2Request()
    //    //      .withBucketName(bucketName)
    //    //      .withPrefix(prefix)
    //    //    val res: ListObjectsV2Result = PersistentS3ClientObject.s3.listObjectsV2(req)
    //
    //    val s3KeysFiltered: List[String] = s3Keys.filter(filterKey)

    val s3KeysFiltered: List[String] = getPaginatedS3Keys(bucketName, prefix)

    println("List of S3 keys built.\n")

    //val rdd: RDD[String] = sc.parallelize(result.getObjectSummaries.map(_.getKey).toList)
    //val rdd: RDD[String] = sc.parallelize(s3Keys)
    // val rdd: RDD[String] = sc.parallelize(s3KeysFiltered)
    val rdd: RDD[String] = sc.parallelize(getPaginatedS3Keys(bucketName, prefix))
      .mapPartitions { it =>
        val s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build()
        it.flatMap { key =>
          Source.fromInputStream(s3.getObject(bucketName, key).getObjectContent).getLines
        }
        //it.flatMap { key =>
        //  Source.fromInputStream(PersistentS3ClientObject.s3.getObject(bucketName, key).getObjectContent).getLines
        //}
      }

    println("RDD defined.\n")

    //    val rdd: RDD[String] = sc.parallelize(keys(PersistentS3ClientObject.s3, "ooni-data", "autoclaved/jsonl/2020-05-31/"))
    //      .mapPartitions { it =>
    //        // val s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build()
    //        it.flatMap { key =>
    //          Source.fromInputStream(PersistentS3ClientObject.s3.getObject(bucket, key).getObjectContent).getLines
    //        }
    //      }

    //    val df: DataFrame = spark.sqlContext.read
    //      .schema(jsonSchemaBase)
    //      .json(rdd.map(identity))
    //      .cache()
    val df: Dataset[OONIBaseRow] = spark.sqlContext.read
      .schema(jsonSchemaBase)
      .json(rdd.toDS)
      .as[OONIBaseRow]
      .filter(col("test_name").isInCollection(testNamesOfInterest))
      .cache()

    println("Dataset defined.\n")

    println(s"DF Row Count: ${df.count()}\n")

    //    println(s"Country Row Count:")
    //    df.groupBy("bucket_date", "test_name")
    //      .count()
    //      .orderBy("bucket_date", "test_name")
    //      .show(100, truncate = false)

    sc.stop()
    spark.stop()

    println("Done.\n")

    println(Calendar.getInstance().getTime.toString + "\n\n")

  }
}
