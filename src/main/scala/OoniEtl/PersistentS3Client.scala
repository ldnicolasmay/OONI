package OoniEtl

import java.io.File

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.typesafe.config.{Config, ConfigFactory}

object PersistentS3Client {

  // Get AWS credentials from aws.conf
  private val awsConfig: Config = ConfigFactory.parseFile(new File("src/main/resources/config/aws.conf"))
  private val awsAccessKeyId: String = awsConfig.getString("aws.awsAccessKeyId")
  private val awsSecretAccessKey: String = awsConfig.getString("aws.awsSecretAccessKey")

  // Define values for S3 client
  private val awsClientRegion: Regions = Regions.US_EAST_1
  private val awsCredentials = new BasicAWSCredentials(awsAccessKeyId, awsSecretAccessKey)

  // Build S3 client
  val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard()
    .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
    .withRegion(awsClientRegion)
    .build()

}
