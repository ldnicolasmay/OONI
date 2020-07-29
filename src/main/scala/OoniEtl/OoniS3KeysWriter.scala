package OoniEtl

import java.io.File

import OoniEtl.PersistentS3Client.s3Client
import com.amazonaws.services.s3.model.{ListObjectsV2Request, ListObjectsV2Result}
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._

object OoniS3KeysWriter {
  def main(args: Array[String]): Unit = {

    /**
     * Gets next page of filtered S3 keys; if results are truncated this is called recursively
     *
     * @param req        S3 objects request, needed for
     * @param res        S3 objects result
     * @param filterKeys Function to filter list of S3 keys
     * @param keys       Existing list of S3 keys
     * @return List of S3 keys
     */
    @scala.annotation.tailrec
    def getNextPageS3Keys(
                           req: ListObjectsV2Request,
                           res: ListObjectsV2Result,
                           filterKeys: String => Boolean = _ => true,
                           keys: List[String] = Nil
                         ): List[String] = {
      val pageKeys = res.getObjectSummaries.map(_.getKey).toList.filter(filterKeys)

      if (res.isTruncated) {
        val nextToken: String = res.getNextContinuationToken
        req.setContinuationToken(nextToken)
        getNextPageS3Keys(req, s3Client.listObjectsV2(req), filterKeys, pageKeys ::: keys)
      } else {
        pageKeys ::: keys
      }
    }

    /**
     * Simpliflies retrieval of potentially paginated S3 keys
     * Returns result of call to getNextPageS3Keys
     *
     * @param bucketName S3 bucket name
     * @param prefix     S3 keys prefix
     * @param filterKeys Function to filter list of S3 keys
     * @return List of S3 keys
     */
    def getPaginatedS3Keys(
                            bucketName: String,
                            prefix: String,
                            filterKeys: String => Boolean
                          ): List[String] = {
      val req: ListObjectsV2Request = new ListObjectsV2Request()
        .withBucketName(bucketName)
        .withPrefix(prefix)
        .withMaxKeys(1000)
      val res: ListObjectsV2Result = s3Client.listObjectsV2(req)

      getNextPageS3Keys(req, res, filterKeys)
    }

    /**
     * Writes list of S3 keys to files after applying filterKeys function
     *
     * @param wd                Working directory
     * @param ooniBucketName    OONI S3 bucket name
     * @param ooniPrefixRoot    OONI S3 prefix root
     * @param filterKeys        Function to filter list of S3 keys
     * @param ooniPrefixPostfix OONI S3 prefix postfix
     */
    def writeS3KeysToFile(
                           wd: os.Path,
                           ooniBucketName: String,
                           ooniPrefixRoot: String,
                           filterKeys: String => Boolean
                         )(ooniPrefixPostfix: String): Unit = {
      val ooniPrefix = ooniPrefixRoot + ooniPrefixPostfix
      val s3KeysFiltered: List[String] = getPaginatedS3Keys(ooniBucketName, ooniPrefix, filterKeys)

      // Write each S3 key to a line
      val fileName: String = s"ooni_s3_keys_${ooniPrefixPostfix}.dat"
      s3KeysFiltered.foreach(s3key => os.write.append(wd / fileName, s3key + "\n"))
      println(s"OONI S3 keys written to ${wd}/${fileName}")
    }

    // Config: Get OONI S3 bucket and prefix from ooni.conf
    val ooniConfig: Config = ConfigFactory.parseFile(new File("src/main/resources/config/ooni.conf"))
    val ooniBucketName: String = ooniConfig.getString("ooni.bucketName")
    val ooniPrefixRoot: String = ooniConfig.getString("ooni.prefixRoot")
    val ooniPrefixDates: List[String] = ooniConfig.getStringList("ooni.prefixPostfixes").toList
    val ooniTargetTestNames: List[String] = ooniConfig.getStringList("ooni.targetTestNames").toList

    val keyFilterRegex: String = ".*(" + ooniTargetTestNames.mkString("|") + ").*"
    val filterKeys = (x: String) => x.matches(keyFilterRegex)

    // Write text files in parallel
    val wd: os.Path = os.pwd / os.RelPath("src/main/resources/data")
    os.makeDir.all(wd)
    ooniPrefixDates.par.foreach(writeS3KeysToFile(wd, ooniBucketName, ooniPrefixRoot, filterKeys)(_))

  }
}
