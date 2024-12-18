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
package org.apache.sedona.core.joinJudgement;

import java.io.Serializable;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.util.LongAccumulator;

/**
 * Unify the interfaces of SQLMetric and LongAccumulator. SQLMetric could be displayed in SQL query
 * detail page and won't be displayed in the stage detail page, while LongAccumulator could be
 * displayed in the stage detail page.
 */
public class SpatialJoinMetric implements Serializable {

  private final SQLMetric sqlMetric;
  private final LongAccumulator accumulator;

  public SpatialJoinMetric(SQLMetric sqlMetric, LongAccumulator accumulator) {
    this.sqlMetric = sqlMetric;
    this.accumulator = accumulator;
  }

  public SpatialJoinMetric() {
    this(null, null);
  }

  public void add(long delta) {
    if (sqlMetric != null) {
      sqlMetric.add(delta);
    }
    if (accumulator != null) {
      accumulator.add(delta);
    }
  }
}
