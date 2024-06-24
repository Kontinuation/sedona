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
package org.apache.sedona.core

import org.apache.sedona.common.enums.FileDataSplitter
import org.apache.sedona.core.enums.{DistanceMetric, GridType, IndexType}
import org.apache.sedona.core.spatialOperator.JoinQuery
import org.apache.sedona.core.spatialPartitioning.ZOrderPartitioner
import org.apache.sedona.core.spatialRDD.PointRDD
import org.apache.sedona.sql.TestBaseScala
import org.scalatest.prop.TableDrivenPropertyChecks.{Table, forAll}
import org.scalatest.prop.TableFor7

import scala.collection.JavaConverters._
import java.nio.file.{Files, Paths}
import scala.io.Source
import scala.util.Using

class KnnJoinQueryTest extends TestBaseScala {

  val testRootPath: String = System.getProperty("user.dir") + "/src/test/resources/knn/"

  case class KnnTestCase(
      id: Int,
      desc: String,
      p: Int,
      k: Int,
      objectLocation: String,
      queryLocation: String,
      resultLocation: String)

  // Function to read and parse the external file
  def readKnnTestCases(filePath: String): Seq[KnnTestCase] = {
    val lines = Using(Source.fromFile(filePath)) { source =>
      source.getLines().drop(1).toList
    }
    lines match {
      case scala.util.Success(l) =>
        l.map { line =>
          val Array(id, desc, p, k, objectLocation, queryLocation, resultLocation) =
            line.split(",")
          KnnTestCase(
            id.toInt,
            desc,
            p.toInt,
            k.toInt,
            objectLocation,
            queryLocation,
            resultLocation)
        }
      case scala.util.Failure(exception) =>
        println(s"Error reading file: ${exception.getMessage}")
        Seq.empty
    }
  }

  val knnTestCasesFilePath: String = testRootPath + "all-test-cases.csv"
  val knnTestCasesList: Seq[KnnTestCase] = readKnnTestCases(knnTestCasesFilePath)

  val knnTestCases: TableFor7[Int, String, Int, Int, String, String, String] = Table(
    ("id", "desc", "p", "k", "objectLocation", "queryLocation", "resultLocation"),
    knnTestCasesList.map(tc =>
      (tc.id, tc.desc, tc.p, tc.k, tc.objectLocation, tc.queryLocation, tc.resultLocation)): _*)

  forAll(knnTestCases) {
    (
        id: Int,
        desc: String,
        p: Int,
        k: Int,
        objectLocation: String,
        queryLocation: String,
        resultLocation: String) =>
      it(s"$id - $desc: p=$p, k=$k") {
        val objectRDD =
          new PointRDD(sc, testRootPath + objectLocation, 0, FileDataSplitter.CSV, true, p)
        val queryRDD =
          new PointRDD(sc, testRootPath + queryLocation, 0, FileDataSplitter.CSV, true, p)

        objectRDD.setNeighborSampleNumber(k)
        objectRDD.spatialPartitioning(GridType.ZORDER)
        queryRDD.spatialPartitioning(
          objectRDD.getPartitioner.asInstanceOf[ZOrderPartitioner].nonOverlappedPartitioner())

        objectRDD.buildIndex(IndexType.RTREE, true)
        queryRDD.buildIndex(IndexType.RTREE, true)

        val knnOutputs = JoinQuery
          .KNNJoinQuery(objectRDD, queryRDD, IndexType.RTREE, k, DistanceMetric.EUCLIDEAN)
          .collect()
          .asScala
          .toList
        val output = knnOutputs.map { case (queryPoint, neighbors) =>
          val neighborsString = neighbors.asScala.mkString(",")
          s"$queryPoint,$neighborsString\n"
        }.mkString

        val expectedOutput =
          new String(Files.readAllBytes(Paths.get(testRootPath + resultLocation)))
        print(output)
        assert(expectedOutput == output)

        // java.lang.Thread.sleep(100*1000) // for debugging
      }
  }
}
