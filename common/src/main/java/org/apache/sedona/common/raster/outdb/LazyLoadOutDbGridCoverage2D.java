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
package org.apache.sedona.common.raster.outdb;

import com.esotericsoftware.kryo.io.UnsafeInput;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.awt.image.RenderedImage;
import java.awt.image.renderable.RenderableImage;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.media.jai.Interpolation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.sedona.common.raster.serde.KryoUtil;
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.grid.GridCoordinates2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.geometry.Envelope2D;
import org.opengis.coverage.CannotEvaluateException;
import org.opengis.coverage.grid.GridCoverage;
import org.opengis.coverage.grid.GridGeometry;
import org.opengis.geometry.DirectPosition;
import org.opengis.geometry.Envelope;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.util.Record;
import org.opengis.util.RecordType;

/**
 * Lazy-loaded OutDbGridCoverage2D is a wrapper for OutDbGridCoverage2D that only reads the
 * referenced raster data when necessary, even when we only need to read the metadata of the raster,
 * such as its width, height and geo-referencing information. This is for avoiding unnecessary data
 * loading when we only want to wrap a URI to a raster, without any other overhead.
 */
public class LazyLoadOutDbGridCoverage2D extends OutDbGridCoverage2D {

  private final OutDbResourcePool.ResourceKey resourceKey;
  private OutDbGridCoverage2D wrapped;

  public LazyLoadOutDbGridCoverage2D(CharSequence name, OutDbResourcePool.ResourceKey resourceKey) {
    super(name);
    this.resourceKey = resourceKey;
    this.wrapped = null;
  }

  public LazyLoadOutDbGridCoverage2D(CharSequence name, Path path, Configuration conf) {
    this(name, new OutDbResourcePool.ResourceKey(path, conf));
  }

  public OutDbGridCoverage2D getWrapped() {
    return wrapped;
  }

  private void ensureWrappedLoaded() {
    if (wrapped != null) {
      return;
    }
    try {
      wrapped = OutDbGridCoverage2D.create(getName(), resourceKey);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public synchronized boolean dispose(boolean force) {
    if (wrapped != null) {
      boolean ret = wrapped.dispose(force);
      if (ret) {
        super.dispose(force);
      }
      return ret;
    } else {
      return super.dispose(force);
    }
  }

  @Override
  public String toString() {
    if (wrapped != null) {
      return String.format("LazyLoadOutDbGridCoverage2D[%s]", wrapped);
    } else {
      return "LazyLoadOutDbGridCoverage2D[not loaded]";
    }
  }

  @Override
  public Path getOutDbPath() {
    return resourceKey.path;
  }

  @Override
  public byte[] getSerializedConfiguration() {
    return resourceKey.serializedConf;
  }

  @Override
  public Map<String, String> getOutDbParams() {
    return resourceKey.params;
  }

  public void writeKryo(UnsafeOutput output, boolean withOutDbConfiguration) {
    KryoUtil.writeUTF8String(output, getName().toString());
    KryoUtil.writeUTF8StringMap(output, resourceKey.params);
    KryoUtil.writeUTF8String(output, resourceKey.path.toString());
    if (withOutDbConfiguration) {
      byte[] serializedConf = resourceKey.serializedConf;
      output.writeInt(serializedConf.length);
      output.writeBytes(serializedConf);
    } else {
      output.writeInt(-1);
    }
  }

  public static LazyLoadOutDbGridCoverage2D readKryo(UnsafeInput in, byte[] serializedConf) {
    String name = KryoUtil.readUTF8String(in);
    Map<String, String> params = KryoUtil.readUTF8StringMap(in);
    String path = KryoUtil.readUTF8String(in);
    int serializedConfLength = in.readInt();
    if (serializedConfLength > 0) {
      // serialized configuration is present, use it instead of user provided conf
      byte[] serializedConfRead = new byte[serializedConfLength];
      in.readBytes(serializedConfRead);
      serializedConf = serializedConfRead;
    } else {
      // serialized configuration is not present, user must provide a configuration to reconstruct
      // the raster
      if (serializedConf == null) {
        throw new IllegalStateException(
            "Configuration was not serialized, cannot restore without user specified configuration");
      }
    }
    OutDbResourcePool.ResourceKey resourceKey =
        new OutDbResourcePool.ResourceKey(new Path(path), serializedConf, params);
    return new LazyLoadOutDbGridCoverage2D(name, resourceKey);
  }

  /// Everything below are just forwarding methods to the wrapped OutDbGridCoverage2D

  @Override
  public GridGeometry2D getGridGeometry() {
    ensureWrappedLoaded();
    return wrapped.getGridGeometry();
  }

  @Override
  public Envelope getEnvelope() {
    ensureWrappedLoaded();
    return wrapped.getEnvelope();
  }

  @Override
  public Envelope2D getEnvelope2D() {
    ensureWrappedLoaded();
    return wrapped.getEnvelope2D();
  }

  @Override
  public CoordinateReferenceSystem getCoordinateReferenceSystem2D() {
    ensureWrappedLoaded();
    return wrapped.getCoordinateReferenceSystem2D();
  }

  @Override
  public int getNumSampleDimensions() {
    ensureWrappedLoaded();
    return wrapped.getNumSampleDimensions();
  }

  @Override
  public GridSampleDimension getSampleDimension(int index) {
    ensureWrappedLoaded();
    return wrapped.getSampleDimension(index);
  }

  @Override
  public GridSampleDimension[] getSampleDimensions() {
    ensureWrappedLoaded();
    return wrapped.getSampleDimensions();
  }

  @Override
  public Interpolation getInterpolation() {
    ensureWrappedLoaded();
    return wrapped.getInterpolation();
  }

  @Override
  public Object evaluate(DirectPosition point) throws CannotEvaluateException {
    ensureWrappedLoaded();
    return wrapped.evaluate(point);
  }

  @Override
  public byte[] evaluate(DirectPosition coord, byte[] dest) throws CannotEvaluateException {
    ensureWrappedLoaded();
    return wrapped.evaluate(coord, dest);
  }

  @Override
  public int[] evaluate(DirectPosition coord, int[] dest) throws CannotEvaluateException {
    ensureWrappedLoaded();
    return wrapped.evaluate(coord, dest);
  }

  @Override
  public float[] evaluate(DirectPosition coord, float[] dest) throws CannotEvaluateException {
    ensureWrappedLoaded();
    return wrapped.evaluate(coord, dest);
  }

  @Override
  public double[] evaluate(DirectPosition coord, double[] dest) throws CannotEvaluateException {
    ensureWrappedLoaded();
    return wrapped.evaluate(coord, dest);
  }

  @Override
  public int[] evaluate(Point2D coord, int[] dest) throws CannotEvaluateException {
    ensureWrappedLoaded();
    return wrapped.evaluate(coord, dest);
  }

  @Override
  public float[] evaluate(Point2D coord, float[] dest) throws CannotEvaluateException {
    ensureWrappedLoaded();
    return wrapped.evaluate(coord, dest);
  }

  @Override
  public double[] evaluate(Point2D coord, double[] dest) throws CannotEvaluateException {
    ensureWrappedLoaded();
    return wrapped.evaluate(coord, dest);
  }

  @Override
  public int[] evaluate(GridCoordinates2D coord, int[] dest) {
    ensureWrappedLoaded();
    return wrapped.evaluate(coord, dest);
  }

  @Override
  public float[] evaluate(GridCoordinates2D coord, float[] dest) {
    ensureWrappedLoaded();
    return wrapped.evaluate(coord, dest);
  }

  @Override
  public double[] evaluate(GridCoordinates2D coord, double[] dest) {
    ensureWrappedLoaded();
    return wrapped.evaluate(coord, dest);
  }

  @Override
  public synchronized String getDebugString(DirectPosition coord) {
    ensureWrappedLoaded();
    return wrapped.getDebugString(coord);
  }

  @Override
  public int[] getOptimalDataBlockSizes() {
    ensureWrappedLoaded();
    return wrapped.getOptimalDataBlockSizes();
  }

  @Override
  public RenderedImage getRenderedImage() {
    ensureWrappedLoaded();
    return wrapped.getRenderedImage();
  }

  @Override
  public RenderableImage getRenderableImage(int xAxis, int yAxis) {
    ensureWrappedLoaded();
    return wrapped.getRenderableImage(xAxis, yAxis);
  }

  @Override
  public void show(String title, int xAxis, int yAxis) {
    ensureWrappedLoaded();
    wrapped.show(title, xAxis, yAxis);
  }

  @Override
  public void show(String title) {
    ensureWrappedLoaded();
    wrapped.show(title);
  }

  @Override
  public void prefetch(Rectangle2D area) {
    ensureWrappedLoaded();
    wrapped.prefetch(area);
  }

  @Override
  public int[] getOutDbBandIndices() {
    ensureWrappedLoaded();
    return wrapped.getOutDbBandIndices();
  }

  @Override
  public List<GridCoverage> getSources() {
    ensureWrappedLoaded();
    return wrapped.getSources();
  }

  @Override
  public int getNumOverviews() {
    ensureWrappedLoaded();
    return wrapped.getNumOverviews();
  }

  @Override
  public GridGeometry getOverviewGridGeometry(int index) throws IndexOutOfBoundsException {
    ensureWrappedLoaded();
    return wrapped.getOverviewGridGeometry(index);
  }

  @Override
  public GridCoverage getOverview(int index) throws IndexOutOfBoundsException {
    ensureWrappedLoaded();
    return wrapped.getOverview(index);
  }

  @Override
  public CoordinateReferenceSystem getCoordinateReferenceSystem() {
    ensureWrappedLoaded();
    return wrapped.getCoordinateReferenceSystem();
  }

  @Override
  public RecordType getRangeType() {
    ensureWrappedLoaded();
    return wrapped.getRangeType();
  }

  @Override
  public Set<Record> evaluate(DirectPosition p, Collection<String> list) {
    ensureWrappedLoaded();
    return wrapped.evaluate(p, list);
  }

  @Override
  public boolean[] evaluate(DirectPosition coord, boolean[] dest) throws CannotEvaluateException {
    ensureWrappedLoaded();
    return wrapped.evaluate(coord, dest);
  }

  @Override
  public void show() {
    ensureWrappedLoaded();
    wrapped.show();
  }

  @Override
  public Locale getLocale() {
    ensureWrappedLoaded();
    return wrapped.getLocale();
  }

  @Override
  public String[] getPropertyNames() {
    ensureWrappedLoaded();
    return wrapped.getPropertyNames();
  }

  @Override
  public String[] getPropertyNames(String prefix) {
    ensureWrappedLoaded();
    return wrapped.getPropertyNames(prefix);
  }

  @Override
  public Class<?> getPropertyClass(String propertyName) {
    ensureWrappedLoaded();
    return wrapped.getPropertyClass(propertyName);
  }

  @Override
  public Object getProperty(String propertyName) {
    ensureWrappedLoaded();
    return wrapped.getProperty(propertyName);
  }

  @Override
  public Map<?, ?> getProperties() {
    ensureWrappedLoaded();
    return wrapped.getProperties();
  }
}
