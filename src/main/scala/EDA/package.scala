import org.apache.spark.sql.SparkSession

package object EDA {

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

}
