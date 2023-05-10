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
import org.apache.sedona.sql.TestBaseScala

class listenerTest extends TestBaseScala {
  val logger = java.util.logging.Logger.getLogger(getClass.getName)
  val userid = sys.env("WHEROBOTS_USERID")
  val awsAccessKey = sys.env("WHEROBOTS_AWS_ACCESSKEY")
  val awsSecretKey = sys.env("WHEROBOTS_AWS_SECRETKEY")
  val awsS3path = sys.env("WHEROBOTS_AWS_S3BUCKET")
  val awsBucketName = awsS3path.split("/")(0)
  val awsBucketPrefix = awsS3path.split("/")(1) + "/"
  val awsRegion = sys.env("WHEROBOTS_AWS_REGION")

  it("Read a single relation") {
    val iterations = 1
    val startTimeMillis = System.currentTimeMillis()
    for (i <- 1 to iterations) {
      var df = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPointInputLocation)
      df = df.selectExpr("ST_Point(cast(_c0 as Decimal(24,20)), cast(_c1 as Decimal(24,20))) as geom")
      df = df.filter("ST_Area(geom) >=0")
      df = df.as("df1").join(df.as("df2")).filter("ST_Distance(df1.geom, df2.geom) <= 0") // Return the points themselves
      assert(df.count() == 1000)
    }
    val endTimeMillis = System.currentTimeMillis()
    val avgDurationSeconds = (endTimeMillis - startTimeMillis) * 1.0 / (1000 * iterations)

//    println(s"avgDuration ${avgDurationSeconds}")

    val s3 = S3Utils.getSyncClient(awsAccessKey, awsSecretKey, awsRegion)
    val logs = S3Utils.listObject(s3, awsBucketName, awsBucketPrefix, logger)
    val objects = logs.contents()
    assert(objects.size() > 0)
  }
}