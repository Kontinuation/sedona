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
package org.apache.spark.sql.sedona_sql.expressions

import org.apache.sedona.sql.utils.GeometrySerializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, Literal, ScalarSubquery}
import org.apache.spark.sql.execution.{ScalarSubquery => ExecutionScalarSubquery}
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types.{AbstractDataType, BooleanType, DataType}
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.prep.{PreparedGeometry, PreparedGeometryFactory}

/**
 * Spatial predicate that uses PreparedGeometry. [[ST_Predicate]] will be transformed to
 * [[ST_PreparedPredicate]] if one side of the expression can be evaluated to a literal, and that
 * geometry literal will be evaluated to a PreparedGeometry and being reused when evaluating this
 * expression with other left hand side geometries.
 *
 * [[ST_PreparedPredicate]] can also simply spatial filter pushdown rules. Since the right hand
 * side is always a literal, the spatial filter pushdown rule only need to check if the left hand
 * side is an attribute reference to the geometry column of the table.
 */
abstract class ST_PreparedPredicate
    extends Expression
    with FoldableExpression
    with ExpectsInputTypes
    with NullIntolerantShim {
  def left: Expression
  def right: Expression

  require(
    right == null || right.isInstanceOf[Literal] || right.isInstanceOf[ScalarSubquery] || right
      .isInstanceOf[ExecutionScalarSubquery],
    "Right side of the expression must be a literal or scalar subquery")

  override def toString: String = s" **${this.getClass.getName}**  "
  override def nullable: Boolean = children.exists(_.nullable)
  override def inputTypes: Seq[AbstractDataType] = Seq(GeometryUDT, GeometryUDT)
  override def dataType: DataType = BooleanType
  override def children: Seq[Expression] = left :: right :: Nil

  lazy val rightGeom: Geometry = {
    val serializedGeom = right.eval().asInstanceOf[Array[Byte]]
    if (serializedGeom == null) {
      null
    } else {
      GeometrySerializer.deserialize(serializedGeom)
    }
  }

  lazy val rightPreparedGeom: PreparedGeometry = {
    if (rightGeom == null) {
      null
    } else {
      val factory = new PreparedGeometryFactory
      factory.create(rightGeom)
    }
  }

  override def eval(inputRow: InternalRow): Any = {
    val leftArray = left.eval(inputRow).asInstanceOf[Array[Byte]]
    if (leftArray == null) {
      null
    } else {
      if (rightPreparedGeom == null) {
        null
      } else {
        val leftGeometry = GeometrySerializer.deserialize(leftArray)
        evalGeom(leftGeometry, rightPreparedGeom)
      }
    }
  }

  def evalGeom(leftGeometry: Geometry, rightPreparedGeom: PreparedGeometry): Boolean

  protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): ST_PreparedPredicate = {
    withNewChildrenInternal(newChildren(0), newChildren(1))
  }

  protected def withNewChildrenInternal(left: Expression, right: Expression): ST_PreparedPredicate
}

case class ST_PreparedContains(left: Expression, right: Expression)
    extends ST_PreparedPredicate
    with CodegenFallback {
  override def evalGeom(left: Geometry, preparedRight: PreparedGeometry): Boolean = {
    preparedRight.within(left)
  }

  override protected def withNewChildrenInternal(left: Expression, right: Expression) = {
    copy(left = left, right = right)
  }
}

case class ST_PreparedIntersects(left: Expression, right: Expression)
    extends ST_PreparedPredicate
    with CodegenFallback {
  override def evalGeom(left: Geometry, preparedRight: PreparedGeometry): Boolean = {
    preparedRight.intersects(left)
  }

  override protected def withNewChildrenInternal(left: Expression, right: Expression) = {
    copy(left = left, right = right)
  }
}

case class ST_PreparedWithin(left: Expression, right: Expression)
    extends ST_PreparedPredicate
    with CodegenFallback {
  override def evalGeom(left: Geometry, preparedRight: PreparedGeometry): Boolean = {
    preparedRight.contains(left)
  }

  override protected def withNewChildrenInternal(left: Expression, right: Expression) = {
    copy(left = left, right = right)
  }
}

case class ST_PreparedCovers(left: Expression, right: Expression)
    extends ST_PreparedPredicate
    with CodegenFallback {
  override def evalGeom(left: Geometry, preparedRight: PreparedGeometry): Boolean = {
    preparedRight.coveredBy(left)
  }

  override protected def withNewChildrenInternal(left: Expression, right: Expression) = {
    copy(left = left, right = right)
  }
}

case class ST_PreparedCoveredBy(left: Expression, right: Expression)
    extends ST_PreparedPredicate
    with CodegenFallback {
  override def evalGeom(left: Geometry, preparedRight: PreparedGeometry): Boolean = {
    preparedRight.covers(left)
  }

  override protected def withNewChildrenInternal(left: Expression, right: Expression) = {
    copy(left = left, right = right)
  }
}

case class ST_PreparedCrosses(left: Expression, right: Expression)
    extends ST_PreparedPredicate
    with CodegenFallback {

  override def evalGeom(left: Geometry, preparedRight: PreparedGeometry): Boolean = {
    preparedRight.crosses(left)
  }

  override protected def withNewChildrenInternal(left: Expression, right: Expression) = {
    copy(left = left, right = right)
  }
}

case class ST_PreparedOverlaps(left: Expression, right: Expression)
    extends ST_PreparedPredicate
    with CodegenFallback {

  override def evalGeom(left: Geometry, preparedRight: PreparedGeometry): Boolean = {
    preparedRight.overlaps(left)
  }

  override protected def withNewChildrenInternal(left: Expression, right: Expression) = {
    copy(left = left, right = right)
  }
}

case class ST_PreparedTouches(left: Expression, right: Expression)
    extends ST_PreparedPredicate
    with CodegenFallback {

  override def evalGeom(left: Geometry, preparedRight: PreparedGeometry): Boolean = {
    preparedRight.touches(left)
  }

  override protected def withNewChildrenInternal(left: Expression, right: Expression) = {
    copy(left = left, right = right)
  }
}

case class ST_PreparedEquals(left: Expression, right: Expression)
    extends ST_PreparedPredicate
    with CodegenFallback {

  override def evalGeom(left: Geometry, preparedRight: PreparedGeometry): Boolean = {
    // We are not using the prepared geometry. However reusing the original geometry saves us some
    // deserialization overhead, so it is expected to be faster than ST_Equals.
    val symDifference = left.symDifference(rightGeom)
    symDifference.isEmpty
  }

  override protected def withNewChildrenInternal(left: Expression, right: Expression) = {
    copy(left = left, right = right)
  }
}

case class ST_PreparedDisjoint(left: Expression, right: Expression)
    extends ST_PreparedPredicate
    with CodegenFallback {

  override def evalGeom(left: Geometry, preparedRight: PreparedGeometry): Boolean = {
    preparedRight.disjoint(left)
  }

  override protected def withNewChildrenInternal(left: Expression, right: Expression) = {
    copy(left = left, right = right)
  }
}

case class ST_PreparedOrderingEquals(left: Expression, right: Expression)
    extends ST_PreparedPredicate
    with CodegenFallback {

  override def evalGeom(left: Geometry, preparedRight: PreparedGeometry): Boolean = {
    left.equalsExact(rightGeom)
  }

  override protected def withNewChildrenInternal(left: Expression, right: Expression) = {
    copy(left = left, right = right)
  }
}
