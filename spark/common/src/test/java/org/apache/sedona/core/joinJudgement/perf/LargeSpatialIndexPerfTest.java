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
package org.apache.sedona.core.joinJudgement.perf;

import static org.mockito.Mockito.when;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.collections4.iterators.BoundedIterator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.core.enums.ExecutionMode;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.enums.JoinType;
import org.apache.sedona.core.joinJudgement.AdaptiveIndexLookupJudgement;
import org.apache.sedona.core.joinJudgement.AdaptiveIndexLookupJudgement.IndexBuildSide;
import org.apache.sedona.core.joinJudgement.AdaptiveIndexLookupJudgement.LocalSpatialJoinExecParams;
import org.apache.sedona.core.spatialOperator.SpatialPredicate;
import org.apache.spark.sedona.core.index.dataformat.GeometryDataItem;
import org.apache.spark.sedona.core.index.dataformat.GeometryDataItemFormat;
import org.apache.spark.unsafe.Platform;
import org.junit.Ignore;
import org.junit.Test;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

/**
 * Performance test for spatial index based join using randomly generated data. The datasets should
 * be generated and written to binary files using {@code generateTestFiles} before running the
 * tests.
 */
@Ignore
public class LargeSpatialIndexPerfTest extends SpatialIndexPerfTestBase {

  /**
   * Generate test files. Please run this prior to running other tests. You need large amount of
   * heap memory (16GB is recommended) for running this. This implementation deliberately holds all
   * generated geometries in memory for measuring their memory footprint.
   *
   * <ul>
   *   <li>rectangles_10m takes 4GB heap memory
   *   <li>polygons_10m takes 8.8GB heap memory
   *   <li>points_10m takes 1.6GB heap memory
   * </ul>
   *
   * @throws IOException if an error occurs while writing the file
   */
  @Test
  public void generateTestFile() throws IOException, InterruptedException {
    for (int k = 0; k < 10; k++) {
      System.gc();
      Thread.sleep(1000);
    }

    // Generate 10x larger datasets
    System.out.println("Generating 10m rectangles");
    long start = System.nanoTime();
    List<Geometry> geometries = generateRandomRectangles(10_000_000, 0, 0.2);
    long end = System.nanoTime();
    System.out.printf("Generated 10m rectangles. Time taken: %f seconds\n", (end - start) / 1e9);
    for (int k = 0; k < 2; k++) {
      System.gc();
      Thread.sleep(5000);
    }
    System.out.println("Writing 10m rectangles to binary files");
    start = System.nanoTime();
    writeGeometriesToBinaryFile(new File("rectangles_10m.bin"), geometries);
    end = System.nanoTime();
    System.out.printf("Written 10m rectangles. Time taken: %f seconds\n", (end - start) / 1e9);

    System.out.println("Generating 10m polygons");
    start = System.nanoTime();
    geometries = generateRandomPolygons(10_000_000, 0, 0.2, 16);
    end = System.nanoTime();
    System.out.printf("Generated 10m polygons. Time taken: %f seconds\n", (end - start) / 1e9);
    for (int k = 0; k < 2; k++) {
      System.gc();
      Thread.sleep(5000);
    }
    System.out.println("Writing 10m polygons to binary files");
    start = System.nanoTime();
    writeGeometriesToBinaryFile(new File("polygons_10m.bin"), geometries);
    end = System.nanoTime();
    System.out.printf("Written 10m polygons. Time taken: %f seconds\n", (end - start) / 1e9);

    System.out.println("Generating 10m points");
    start = System.nanoTime();
    geometries = generateRandomPoints(10_000_000, 1);
    end = System.nanoTime();
    System.out.printf("Generated 10m points. Time taken: %f seconds\n", (end - start) / 1e9);
    for (int k = 0; k < 2; k++) {
      System.gc();
      Thread.sleep(5000);
    }
    System.out.println("Writing 10m points to binary files");
    start = System.nanoTime();
    writeGeometriesToBinaryFile(new File("points_10m.bin"), geometries);
    end = System.nanoTime();
    System.out.printf("Written 10m points. Time taken: %f seconds\n", (end - start) / 1e9);
  }

  /**
   * This test will consume 8GB heap memory. Please make sure you have enough memory configured for
   * JVM before running this test.
   */
  @Test
  public void inMemRectanglePoint() {
    perfInMemoryLargeSpatialIndex("rectangles_10m.bin", "points_10m.bin");
  }

  /**
   * This test will consume at-least 3GB heap memory. Please make sure you have enough memory
   * configured for JVM before running this test. We have not restricted the amount of allocatable
   * memory for this test so it won't trigger spilling.
   */
  @Test
  public void externalNoSpillingRectanglePoint() {
    perfExternalLargeSpatialIndex("rectangles_10m.bin", "points_10m.bin");
  }

  /**
   * This test limits allocatable memory to be 500MB. This will trigger the spilling of the index.
   * 2GB or even 1GB heap is enough for running this.
   */
  @Test
  public void externalSpillingRectanglePoint() {
    perfExternalLargeSpatialIndexSpilling("rectangles_10m.bin", "points_10m.bin");
  }

  /**
   * This test will consume LOTS of heap memory (24GB is not enough). Please make sure you have
   * enough memory configured for JVM before running this test.
   */
  @Test
  public void inMemPolygonPoint() {
    perfInMemoryLargeSpatialIndex("polygons_10m.bin", "points_10m.bin");
  }

  /**
   * This test will consume at-least 4GB heap memory. Please make sure you have enough memory
   * configured for JVM before running this test. We have not restricted the amount of allocatable
   * memory for this test so it won't trigger spilling.
   */
  @Test
  public void externalNoSpillingPolygonPoint() {
    perfExternalLargeSpatialIndex("polygons_10m.bin", "points_10m.bin");
  }

  /**
   * This test limits allocatable memory to be 500MB. This will trigger the spilling of the index.
   * 2GB or even 1GB heap is enough for running this.
   */
  @Test
  public void externalSpillingPolygonPoint() {
    perfExternalLargeSpatialIndexSpilling("polygons_10m.bin", "points_10m.bin");
  }

  /**
   * This test will consume 8GB heap memory. Please make sure you have enough memory configured for
   * JVM before running this test.
   */
  private void perfInMemoryLargeSpatialIndex(String leftPath, String rightPath) {
    LocalSpatialJoinExecParams param =
        new LocalSpatialJoinExecParams(
            IndexType.RTREE, IndexBuildSide.LEFT, ExecutionMode.PREPARE_BUILD, null, null, null);
    AdaptiveIndexLookupJudgement<Geometry, Geometry> judgement =
        new AdaptiveIndexLookupJudgement<>(
            SpatialPredicate.INTERSECTS, JoinType.INNER, Collections.singletonList(param), null);

    for (int i = 0; i < 10; i++) {
      runSpatialJoin(judgement, leftPath, rightPath);
    }
  }

  private void perfExternalLargeSpatialIndex(String leftPath, String rightPath) {
    LocalSpatialJoinExecParams param =
        new LocalSpatialJoinExecParams(
            IndexType.RTREE, IndexBuildSide.LEFT, ExecutionMode.PREPARE_BUILD, null, null, null);
    AdaptiveIndexLookupJudgement<Geometry, Geometry> judgement =
        new AdaptiveIndexLookupJudgement<>(
            SpatialPredicate.INTERSECTS,
            JoinType.INNER,
            Collections.singletonList(param),
            sedonaConf);
    judgement.setSparkEnv(sparkEnv);
    judgement.setTaskContext(taskContext);

    for (int i = 0; i < 10; i++) {
      runSpatialJoin(judgement, leftPath, rightPath);
    }
  }

  /**
   * This test limits allocatable memory to be 500MB. This will trigger the spilling of the index.
   * 2GB or even 1GB heap is enough for running this.
   */
  private void perfExternalLargeSpatialIndexSpilling(String leftPath, String rightPath) {
    LocalSpatialJoinExecParams param =
        new LocalSpatialJoinExecParams(
            IndexType.RTREE, IndexBuildSide.LEFT, ExecutionMode.PREPARE_BUILD, null, null, null);
    AdaptiveIndexLookupJudgement<Geometry, Geometry> judgement =
        new AdaptiveIndexLookupJudgement<>(
            SpatialPredicate.INTERSECTS,
            JoinType.INNER,
            Collections.singletonList(param),
            sedonaConf);
    judgement.setSparkEnv(sparkEnv);
    judgement.setTaskContext(taskContext);
    memoryManager.limit(1024 * 1024 * 500);

    for (int i = 0; i < 10; i++) {
      runSpatialJoin(judgement, leftPath, rightPath);
    }
  }

  private void runSpatialJoin(
      AdaptiveIndexLookupJudgement<Geometry, Geometry> judgement,
      String leftPath,
      String rightPath) {
    int count = 0;
    long start = System.nanoTime();
    try (BinaryFileGeometryIterator iterLeft = new BinaryFileGeometryIterator(new File(leftPath))) {
      try (BinaryFileGeometryIterator iterRight =
          new BinaryFileGeometryIterator(new File(rightPath))) {
        Iterator<Pair<Geometry, Geometry>> results = judgement.call(0, iterLeft, iterRight);
        while (results.hasNext()) {
          results.next();
          count += 1;
        }
      }
    }

    long end = System.nanoTime();
    System.out.printf("Time taken: %f seconds, count: %d\n", (end - start) / 1e9, count);
    cleanUpResources();
  }

  @Test
  public void perfLoadSerializedData() {
    for (int k = 0; k < 10; k++) {
      long start = System.nanoTime();
      Envelope bound = new Envelope();
      int count = 0;
      try (BinaryFileGeometryIterator iter =
          new BinaryFileGeometryIterator(new File("polygons_10m.bin"))) {
        while (iter.hasNext()) {
          Geometry geom = iter.next();
          bound.expandToInclude(geom.getEnvelopeInternal());
          count += 1;
        }
      }
      long end = System.nanoTime();
      System.out.printf(
          "Read left side. Time taken: %f seconds, count: %d, count: %s\n",
          (end - start) / 1e9, count, bound);
      cleanUpResources();
    }
  }

  @Test
  public void perfLoadSerializedData2() {
    GeometryDataItemFormat format = new GeometryDataItemFormat();
    for (int k = 0; k < 10; k++) {
      long start = System.nanoTime();
      Envelope bound = new Envelope();
      int count = 0;
      try (BinaryFileGeometryIterator iter =
          new BinaryFileGeometryIterator(new File("polygons_10m.bin"))) {
        while (iter.hasNext()) {
          byte[] serialized = iter.nextSerialized();
          bound.expandToInclude(format.extractEnvelope(serialized));
          count += 1;
        }
      }
      long end = System.nanoTime();
      System.out.printf(
          "Read left side. Time taken: %f seconds, count: %d, count: %s\n",
          (end - start) / 1e9, count, bound);
      cleanUpResources();
    }
  }

  @Test
  public void inMemSpatialJoinVaryingIndexSize() {
    LocalSpatialJoinExecParams param =
        new LocalSpatialJoinExecParams(
            IndexType.RTREE, IndexBuildSide.LEFT, ExecutionMode.PREPARE_BUILD, null, null, null);
    AdaptiveIndexLookupJudgement<Geometry, Geometry> judgement =
        new AdaptiveIndexLookupJudgement<>(
            SpatialPredicate.INTERSECTS, JoinType.INNER, Collections.singletonList(param), null);

    measurePerformanceUsingVariousBounds(judgement);
  }

  @Test
  public void noSpillingSpatialJoinVaryingIndexSize() {
    LocalSpatialJoinExecParams param =
        new LocalSpatialJoinExecParams(
            IndexType.RTREE, IndexBuildSide.LEFT, ExecutionMode.PREPARE_BUILD, null, null, null);
    AdaptiveIndexLookupJudgement<Geometry, Geometry> judgement =
        new AdaptiveIndexLookupJudgement<>(
            SpatialPredicate.INTERSECTS,
            JoinType.INNER,
            Collections.singletonList(param),
            sedonaConf);
    judgement.setSparkEnv(sparkEnv);
    judgement.setTaskContext(taskContext);

    measurePerformanceUsingVariousBounds(judgement);
  }

  @Test
  public void spillingSpatialJoinVaryingIndexSize() {
    when(sedonaConf.forceSpillExternalSpatialIndex()).thenReturn(true);
    try {
      LocalSpatialJoinExecParams param =
          new LocalSpatialJoinExecParams(
              IndexType.RTREE, IndexBuildSide.LEFT, ExecutionMode.PREPARE_BUILD, null, null, null);
      AdaptiveIndexLookupJudgement<Geometry, Geometry> judgement =
          new AdaptiveIndexLookupJudgement<>(
              SpatialPredicate.INTERSECTS,
              JoinType.INNER,
              Collections.singletonList(param),
              sedonaConf);
      judgement.setSparkEnv(sparkEnv);
      judgement.setTaskContext(taskContext);
      memoryManager.limit(1024 * 1024 * 500);

      measurePerformanceUsingVariousBounds(judgement);
    } finally {
      when(sedonaConf.forceSpillExternalSpatialIndex()).thenReturn(false);
    }
  }

  private void measurePerformanceUsingVariousBounds(
      AdaptiveIndexLookupJudgement<Geometry, Geometry> judgement) {
    String leftPath = "rectangles_10m.bin";
    String rightPath = "points_10m.bin";
    System.out.println("Rectangles vs Points");
    for (int bound = 10000; bound < 12000000; bound *= 2) {
      System.out.printf("Bound: %d\n", bound);
      measurePerformanceBounded(judgement, leftPath, rightPath, bound, 10000000);
    }

    leftPath = "polygons_10m.bin";
    System.out.println("Polygons vs Points");
    for (int bound = 10000; bound < 6000000; bound *= 2) {
      System.out.printf("Bound: %d\n", bound);
      measurePerformanceBounded(judgement, leftPath, rightPath, bound, 10000000);
    }
  }

  private void measurePerformanceBounded(
      AdaptiveIndexLookupJudgement<Geometry, Geometry> judgement,
      String leftPath,
      String rightPath,
      int leftBound,
      int rightBound) {
    for (int i = 0; i < 3; i++) {
      System.gc();
      int count = 0;
      long start = System.nanoTime();
      long usedHeapMemory;
      try (BinaryFileGeometryIterator iterLeft =
          new BinaryFileGeometryIterator(new File(leftPath))) {
        try (BinaryFileGeometryIterator iterRight =
            new BinaryFileGeometryIterator(new File(rightPath))) {
          BoundedIterator<Geometry> boundedIterLeft =
              IteratorUtils.boundedIterator(iterLeft, leftBound);
          BoundedIterator<Geometry> boundedIterRight =
              IteratorUtils.boundedIterator(iterRight, rightBound);
          Iterator<Pair<Geometry, Geometry>> results =
              judgement.call(0, boundedIterLeft, boundedIterRight);
          usedHeapMemory = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
          while (results.hasNext()) {
            results.next();
            count += 1;
          }
        }
      }

      long end = System.nanoTime();
      System.out.printf(
          "Time taken: %f seconds, heap mem: %d MB, count: %d\n",
          (end - start) / 1e9, usedHeapMemory / 1024 / 1024, count);
      cleanUpResources();
    }
  }

  private static void writeGeometriesToBinaryFile(File file, List<Geometry> geometries)
      throws IOException {
    GeometryDataItemFormat format = new GeometryDataItemFormat();
    try (BufferedOutputStream out =
        new BufferedOutputStream(Files.newOutputStream(file.toPath()))) {
      byte[] dataSizeBuffer = new byte[4];
      for (Geometry geometry : geometries) {
        byte[] data = format.serialize(new GeometryDataItem(geometry));
        Platform.putInt(dataSizeBuffer, Platform.BYTE_ARRAY_OFFSET, data.length);
        out.write(dataSizeBuffer);
        out.write(data);
      }
    }
  }

  private void cleanUpResources() {
    taskContext.runTaskCompletionListeners();
  }

  private static class BinaryFileGeometryIterator implements Iterator<Geometry>, AutoCloseable {
    BufferedInputStream is;
    DataInputStream in;
    GeometryDataItemFormat format;

    boolean isDataBufferValid;
    byte[] dataBuffer;
    byte[] dataLenBuffer;

    BinaryFileGeometryIterator(File file) {
      try {
        is = new BufferedInputStream(Files.newInputStream(file.toPath()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      in = new DataInputStream(is);
      format = new GeometryDataItemFormat();
      isDataBufferValid = false;
      dataBuffer = null;
      dataLenBuffer = new byte[4];
    }

    @Override
    public boolean hasNext() {
      if (isDataBufferValid) {
        return true;
      }
      try {
        tryLoadNext();
        return true;
      } catch (EOFException e) {
        isDataBufferValid = false;
        return false;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Geometry next() {
      if (!isDataBufferValid) {
        try {
          tryLoadNext();
        } catch (EOFException e) {
          throw new NoSuchElementException();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      Geometry result = format.deserializeToGeometry(dataBuffer);
      isDataBufferValid = false;
      return result;
    }

    public byte[] nextSerialized() {
      if (!isDataBufferValid) {
        try {
          tryLoadNext();
        } catch (EOFException e) {
          throw new NoSuchElementException();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      isDataBufferValid = false;
      return dataBuffer;
    }

    void tryLoadNext() throws IOException {
      in.readFully(dataLenBuffer);
      int dataLen = Platform.getInt(dataLenBuffer, Platform.BYTE_ARRAY_OFFSET);
      if (dataBuffer == null || dataBuffer.length < dataLen) {
        dataBuffer = new byte[dataLen * 2];
      }
      in.readFully(dataBuffer, 0, dataLen);
      isDataBufferValid = true;
    }

    @Override
    public void close() {
      try {
        in.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
