/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.spark.sql.sedona_sql.io.raster

import org.apache.hadoop.fs._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.OutputCommitter
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory

class DirectOutputCommitterFactory extends PathOutputCommitterFactory {
  override def createOutputCommitter(
      outputPath: Path,
      context: TaskAttemptContext): PathOutputCommitter =
    new DirectPathOutputCommitter(outputPath, context)
}

trait DirectOutputCommitterTrait extends OutputCommitter {
  override def setupJob(jobContext: JobContext): Unit = {
    val outputPath = FileOutputFormat.getOutputPath(jobContext)
    if (outputPath != null) {
      val fs = outputPath.getFileSystem(jobContext.getConfiguration)
      if (!fs.exists(outputPath)) {
        fs.mkdirs(outputPath)
      }
    }
  }

  override def commitJob(jobContext: JobContext): Unit = {
    val outputPath = FileOutputFormat.getOutputPath(jobContext)
    if (outputPath != null) {
      val fs = outputPath.getFileSystem(jobContext.getConfiguration)
      // True if the job requires output.dir marked on successful job.
      // Note that by default it is set to true.
      if (jobContext.getConfiguration.getBoolean(
          "mapreduce.fileoutputcommitter.marksuccessfuljobs",
          true)) {
        val markerPath = new Path(outputPath, "_SUCCESS")
        fs.create(markerPath).close()
      }
    }
  }

  override def setupTask(taskContext: TaskAttemptContext): Unit = ()
  override def needsTaskCommit(taskContext: TaskAttemptContext): Boolean = false
  override def commitTask(taskContext: TaskAttemptContext): Unit = ()
  override def abortTask(taskContext: TaskAttemptContext): Unit = ()
}

class DirectOutputCommitter extends DirectOutputCommitterTrait

class DirectPathOutputCommitter(outputPath: Path, context: JobContext)
    extends PathOutputCommitter(outputPath, context)
    with DirectOutputCommitterTrait {

  override def getOutputPath: Path = outputPath
  override def getWorkPath: Path = outputPath
}
