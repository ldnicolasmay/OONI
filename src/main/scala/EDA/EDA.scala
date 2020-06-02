package EDA

import com.typesafe.config.ConfigFactory
import java.io.File

import org.apache.spark.sql.{DataFrame, SparkSession}

object EDA {
  def main(args: Array[String]): Unit = {

    // Get AWS credentials from resources/aws.conf
    val awsConfig = ConfigFactory.parseFile(new File("src/main/resources/aws.conf"))
    val awsAccessKeyId = awsConfig.getString("aws.awsAccessKeyId")
    val awsSecretAccessKey = awsConfig.getString("aws.awsSecretAccessKey")

    // Get Spark session
    val spark: SparkSession = createSparkSession(awsAccessKeyId, awsSecretAccessKey)

    // Define endpoints
    val inputEndpoint = "s3a://ooni-data/autoclaved/jsonl/2020-05-28/" // "*-ndt-*-0.2.0-probe.json"
    val outputEndpoint = "s3a://ldnicolasmay/"

    val probeJson =
      // "20200527T151248Z-CL-AS6471-ndt-20200528T015745Z_AS6471_3sctrzdq16iOBLph9dBLUhR5sSiInoNwlI4XMu9fEZrdt3IEtS-0.2.0-probe.json"
    "20200527*-CL-*-ndt-*-0.2.0-probe.json"

    val initData = inputEndpoint + probeJson

    val initDF: DataFrame = spark.read
      .json(initData)
    initDF.printSchema()
    initDF.show(10)

  }
}
