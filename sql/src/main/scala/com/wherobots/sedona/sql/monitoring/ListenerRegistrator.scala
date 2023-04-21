/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wherobots.sedona.sql.monitoring

import com.wherobots.sedona.common.monitoring.{CloudWatchUtils, S3Utils}
import org.apache.log4j.Logger
import org.apache.spark.sql.{RuntimeConfig, SparkSession}
import software.amazon.awssdk.services.s3.model.S3Exception

object ListenerRegistrator {
  val logger = Logger.getLogger(getClass.getName)

  def registerAll(sparkSession: SparkSession):Unit = {
    val conf = sparkSession.conf
    val listeners = createListeners(conf)
    sparkSession.sparkContext.addSparkListener(listeners._1)
    sparkSession.listenerManager.register(listeners._2)
  }

  def unregisterAll(sparkSession: SparkSession): Unit = {
    val conf = sparkSession.conf
    val listeners = createListeners(conf)
    sparkSession.sparkContext.removeSparkListener(listeners._1)
    sparkSession.listenerManager.unregister(listeners._2)
  }

  def createListeners(conf:RuntimeConfig): (IoListener, SqlListener) = {
    var userid = ""
    var awsAccessKey = ""
    var awsSecretKey = ""
    var awsS3path = ""
    var awsRegion = ""
    var product = ""
    // Fetch data from Spark RuntimeConfig
    // These keys are supposed to exist in the environment
    // If not, it will throw RuntimeException
    // Catch the original exception to hide key info
    try {
      userid = conf.get("wherobots.userid")
      awsAccessKey = conf.get("wherobots.aws.accesskey")
      awsSecretKey = conf.get("wherobots.aws.secretkey")
      awsS3path = conf.get("wherobots.aws.s3bucket")
      awsRegion = conf.get("wherobots.aws.region")
      product = conf.get("wherobots.product", "unknown wherobots product")
    }
    catch {
          // Fetch data from System Environment
          // Usually these values should be set by Yarn appMasterEnv or K8S driverEnv
          // Or by the user manually
      case e1: NoSuchElementException => {
        try {
          userid = sys.env("WHEROBOTS_USERID")
          awsAccessKey = sys.env("WHEROBOTS_AWS_ACCESSKEY")
          awsSecretKey = sys.env("WHEROBOTS_AWS_SECRETKEY")
          awsS3path = sys.env("WHEROBOTS_AWS_S3BUCKET")
          awsRegion = sys.env("WHEROBOTS_AWS_REGION")
          product = sys.env.getOrElse("WHEROBOTS_PRODUCT", "unknown wherobots product")
        }
        catch {
              // If the above two methods fail, try to fetch data from Spark RuntimeConfig
          case e2: NoSuchElementException => {
            try {
              userid = conf.get("spark.yarn.appMasterEnv.WHEROBOTS_USERID")
              awsAccessKey = conf.get("spark.yarn.appMasterEnv.WHEROBOTS_AWS_ACCESSKEY")
              awsSecretKey = conf.get("spark.yarn.appMasterEnv.WHEROBOTS_AWS_SECRETKEY")
              awsS3path = conf.get("spark.yarn.appMasterEnv.WHEROBOTS_AWS_S3BUCKET")
              awsRegion = conf.get("spark.yarn.appMasterEnv.WHEROBOTS_AWS_REGION")
              product = conf.get("spark.yarn.appMasterEnv.WHEROBOTS_PRODUCT", "unknown wherobots product")
            }
            catch {
              case e3: NoSuchElementException => {
                {
                  try {
                    userid = conf.get("spark.kubernetes.driverEnv.WHEROBOTS_USERID")
                    awsAccessKey = conf.get("spark.kubernetes.driverEnv.WHEROBOTS_AWS_ACCESSKEY")
                    awsSecretKey = conf.get("spark.kubernetes.driverEnv.WHEROBOTS_AWS_SECRETKEY")
                    awsS3path = conf.get("spark.kubernetes.driverEnv.WHEROBOTS_AWS_S3BUCKET")
                    awsRegion = conf.get("spark.kubernetes.driverEnv.WHEROBOTS_AWS_REGION")
                    product = conf.get("spark.kubernetes.driverEnv.WHEROBOTS_PRODUCT", "unknown wherobots product")
                  }
                  catch {
                        // Only if all the above methods fail, throw the exception
                    case e4: NoSuchElementException => {
                      throw new RuntimeException("Your code is not running in a Wherobots managed environment!")
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    val bucketName = awsS3path.split("/")(0)
    val bucketPrefix = awsS3path.split("/")(1) // Get the log folder name in the bucket
//    val s3ClientAsync = S3Utils.getAsyncClient(awsAccessKey, awsSecretKey, awsRegion)
    val s3Client = S3Utils.getSyncClient(awsAccessKey, awsSecretKey, awsRegion)
    try {
      S3Utils.putObject(s3Client, bucketName, bucketPrefix + "/_SUCCESS", "")
      logger.info("Successfully verified your Wherobots audit credential!")
    }
    catch {
      case e: S3Exception => {
        throw new RuntimeException("Your Wherobots audit credential is not valid!")
      }
    }
    val cloudwatchClient = CloudWatchUtils.getSyncClient(awsAccessKey, awsSecretKey, awsRegion)
    // Get the version of individual product
    val dimensionDataPoints = new java.util.HashMap[String, String]()
    product.split("\\+").foreach(productSeg => {
      val productSegInfo = productSeg.split("=")
      dimensionDataPoints.put(productSegInfo(0), productSegInfo(1))
    })
    (new IoListener(userid, bucketName, bucketPrefix, s3Client, cloudwatchClient, product, dimensionDataPoints),
      new SqlListener(userid, bucketName, bucketPrefix, s3Client, cloudwatchClient, product, dimensionDataPoints))
  }
}