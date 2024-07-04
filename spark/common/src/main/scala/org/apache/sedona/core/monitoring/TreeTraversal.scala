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
package org.apache.sedona.core.monitoring

import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.execution.QueryExecution

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

object TreeTraversal {

  /**
   * Execute the query execution plan and return a list of function calls Known issues:
   *   1. this method does not capture the function calls in the arguments of a function call For
   *      example, RS_Envelope(RS_FromGeoTiff(content)) will only capture RS_Envelope 2. this
   *      method does not capture operations used in Havasu CRUD operations
   * @param qe
   * @return
   */
  def execute(qe: QueryExecution): ListBuffer[String] = {
    val nodeNames = ListBuffer[String]()
    // Traverse the logical plan to get function call names
    getNodeNames(qe.analyzed, nodeNames, isPhyiscalPlan = false)
    // Traverse the physical plan to get the join strategy
    getNodeNames(qe.sparkPlan, nodeNames, isPhyiscalPlan = true)
    nodeNames
  }

  /**
   * Extract all the node names from a plan. Each plan object is the root node of a tree Each node
   * has a list of children. The leaf level node might have a list of arguments.
   *
   * @param tn
   * @param nodeNames
   * @param isPhyiscalPlan
   */
  private def getNodeNames(
      tn: TreeNode[_],
      nodeNames: ListBuffer[String],
      isPhyiscalPlan: Boolean): Unit = {
    extractSedonaClass(tn.getClass.toString, nodeNames, isPhyiscalPlan)
    getArgNodeNames(tn.productIterator, nodeNames, isPhyiscalPlan)
    tn.children.foreach(child => {
      getNodeNames(child.asInstanceOf[TreeNode[_]], nodeNames, isPhyiscalPlan)
    })
  }

  /**
   * Extract all the node names from the arguments of a node Sedona classes are in the form of
   * org.apache.spark.sql.sedona_sql.[class name] Sedona classes are usually put in the arguments
   * of a node
   *
   * @param args
   * @param nodeNames
   * @param isPhysicalPlan
   */
  private def getArgNodeNames(
      args: Iterator[Any],
      nodeNames: ListBuffer[String],
      isPhysicalPlan: Boolean): Unit =
    args.foreach {
      case tn: TreeNode[_] =>
        Nil // Skip the tree node itself otherwise results will be duplicated
      case seq: Seq[_] =>
        seq.foreach(f => extractSedonaClass(f.toString, nodeNames, isPhysicalPlan)) :: Nil
      case set: Set[_] =>
        set.foreach(f => extractSedonaClass(f.toString, nodeNames, isPhysicalPlan)) :: Nil
      case array: Array[_] =>
        array.foreach(f => extractSedonaClass(f.toString, nodeNames, isPhysicalPlan)) :: Nil
      case other =>
        if (other != null) {
          extractSedonaClass(other.toString, nodeNames, isPhysicalPlan) :: Nil
        } else {
          Nil
        }
    }

  /**
   * Extract the Sedona class name from a string A typical Sedona class name starts with
   * org.apache.spark.sql.sedona_sql The class name is the last part of the string The entire
   * string can only have the following characters: a-z, A-Z, 0-9, ., _
   *
   * In a logical plan, we capture the following keywords:
   *   1. ST_Envelope_Aggr, ST_Intersection_Aggr, ST_Union_Aggr 2. binaryFile, raster, geoparquet
   *
   * In a physical plan, we capture the following keywords:
   * org.apache.spark.sql.sedona_sql.strategy.[class name]
   * org.apache.spark.sql.sedona_sql.expressions.[class name]
   *
   * @param inputStr
   * @param nodeNames
   * @param isPhyiscalPlan
   */
  private def extractSedonaClass(
      inputStr: String,
      nodeNames: ListBuffer[String],
      isPhyiscalPlan: Boolean): Unit = {
    if (isPhyiscalPlan) {
      val pattern: Regex = """org\.apache\.spark\.sql\.sedona_sql\.[a-zA-Z0-9\._]+""".r
      pattern
        .findAllMatchIn(inputStr)
        .foreach(f => {
          val result = f.matched.split("\\.").last.toLowerCase()
          if (!result.endsWith("aggr")) nodeNames += result
        })
    } else {
      val pattern =
        """(org\.apache\.spark\.sql\.sedona_sql\.expressions\.ST_Envelope_Aggr|org\.apache\.spark\.sql\.sedona_sql\.expressions\.ST_Intersection_Aggr|org\.apache\.spark\.sql\.sedona_sql\.expressions\.ST_Union_Aggr)""".r
      pattern
        .findAllMatchIn(inputStr)
        .foreach(f => nodeNames += f.matched.split("\\.").last.toLowerCase())
      val pattern2: Regex = """(binaryFile|raster|geoparquet)""".r
      pattern2.findAllMatchIn(inputStr).foreach(f => nodeNames += f.matched.toLowerCase())
    }
  }
}
