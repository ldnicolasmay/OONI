package RDDTutorial

import java.io.File
import java.nio.charset.{Charset, StandardCharsets}

import com.typesafe.config.ConfigFactory
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream
import org.apache.spark.input.PortableDataStream
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try


object RDDTutorial {
  def main(args: Array[String]): Unit = {

    // Get AWS credentials from src/main/resources/aws.conf
    val awsConfig = ConfigFactory.parseFile(new File("src/main/resources/aws.conf"))
    val awsAccessKeyId = awsConfig.getString("aws.awsAccessKeyId")
    val awsSecretAccessKey = awsConfig.getString("aws.awsSecretAccessKey")

    val conf = new SparkConf()
      .setAppName("RDDTutorial")
      .setMaster("local[*]") // comment out when running with spark-submit
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.s3a.access.key", awsAccessKeyId)
    sc.hadoopConfiguration.set("fs.s3a.secret.key", awsSecretAccessKey)

    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)

    val distDataSum = distData.reduce((a, b) => a + b)
    println(distDataSum)

    val inputEndpoint = "s3a://ooni-data/autoclaved/jsonl/2020-05-28/"
    val probeJson =
      "20200527T151248Z-CL-AS6471-ndt-20200528T015745Z_AS6471_3sctrzdq16iOBLph9dBLUhR5sSiInoNwlI4XMu9fEZrdt3IEtS-0.2.0-probe.json"
    val distFile = sc.textFile(inputEndpoint + probeJson)
    println("JSON: " + distFile.map(s => s.length).reduce((a, b) => a + b))

    // val lines = sc.textFile(inputEndpoint + probeJson)
    // val lineLengths = lines.map(s => s.length)
    // lineLengths.persist() // same as lineLengths.cache() ?

    val inputEndpontLZ = "s3://ooni-data/autoclaved/jsonl.tar.lz4/2020-05-28/"
    val probeJsonLZ = "web_connectivity.00.tar.lz4"

    sc.stop()

  }
}
