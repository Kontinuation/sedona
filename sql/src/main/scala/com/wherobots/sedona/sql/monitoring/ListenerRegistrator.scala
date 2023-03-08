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

import com.wherobots.sedona.common.monitoring.S3Utils
import org.apache.spark.sql.{RuntimeConfig, SparkSession}

object ListenerRegistrator {

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
      product = conf.get("wherobots.environment", "unknown wherobots product")
    }
    catch {
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
          case e2: NoSuchElementException => throw new RuntimeException("Your code is not running in a Wherobots managed environment!")
        }
      }
    }
    val bucketName = awsS3path.split("/")(0)
    val bucketPrefix = awsS3path.split("/")(1) // Get the log folder name in the bucket
    val s3Client = S3Utils.getAsyncClient(awsAccessKey, awsSecretKey, awsRegion)
    (new IoListener(userid, bucketName, bucketPrefix, s3Client, product), new SqlListener(userid, bucketName, bucketPrefix, s3Client, product))
  }
}