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
package org.apache.sedona.common.geometrySerde;

import org.apache.sedona.common.enums.GeometryType;
import org.apache.sedona.common.geometrySerde.SerializedCoordinateFilters.StatisticsCollector;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.CoordinateXYM;
import org.locationtech.jts.geom.CoordinateXYZM;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

public class SerializedCoordinateFilterTest {
  private static final GeometryFactory gf = new GeometryFactory();
  private static final StatisticsCollector filter = new StatisticsCollector();

  private void testFilter(Geometry geom) {
    Envelope expectedEnvelope = geom.getEnvelopeInternal();
    int expectedNumCoordinates = geom.getNumPoints();
    GeometryType expectedGeometryType;
    if (geom instanceof Point) {
      expectedGeometryType = GeometryType.POINT;
    } else if (geom instanceof MultiPoint) {
      expectedGeometryType = GeometryType.MULTIPOINT;
    } else if (geom instanceof LineString) {
      expectedGeometryType = GeometryType.LINESTRING;
    } else if (geom instanceof MultiLineString) {
      expectedGeometryType = GeometryType.MULTILINESTRING;
    } else if (geom instanceof Polygon) {
      expectedGeometryType = GeometryType.POLYGON;
    } else if (geom instanceof MultiPolygon) {
      expectedGeometryType = GeometryType.MULTIPOLYGON;
    } else if (geom instanceof GeometryCollection) {
      expectedGeometryType = GeometryType.GEOMETRYCOLLECTION;
    } else {
      throw new IllegalArgumentException(
          "Unsupported geometry type: " + geom.getClass().getSimpleName());
    }

    byte[] bytes = GeometrySerializer.serialize(geom);
    GeometryBuffer buffer = GeometryBufferFactory.wrap(bytes);
    filter.reset();
    filter.apply(buffer);

    Assert.assertEquals(expectedGeometryType, filter.geometryType);
    Assert.assertEquals(expectedNumCoordinates, filter.numCoordinates);
    Assert.assertEquals(expectedEnvelope, filter.getEnvelope());
  }

  @Test
  public void testPoint() {
    testFilter(gf.createPoint());
    testFilter(gf.createPoint(new Coordinate(1, 2)));
    testFilter(gf.createPoint(new CoordinateXY(1, 2)));
    testFilter(gf.createPoint(new Coordinate(1, 2, 3)));
    testFilter(gf.createPoint(new CoordinateXYM(1, 2, 3)));
    testFilter(gf.createPoint(new CoordinateXYZM(1, 2, 3, 4)));
  }

  @Test
  public void testMultiPoint() {
    testFilter(gf.createMultiPoint());

    MultiPoint multiPoint =
        gf.createMultiPointFromCoords(
            new Coordinate[] {
              new Coordinate(1, 2), new Coordinate(3, 4), new Coordinate(5, 6),
            });
    testFilter(multiPoint);

    Point[] points =
        new Point[] {
          gf.createPoint(new Coordinate(1, 2)),
          gf.createPoint(),
          gf.createPoint(new Coordinate(3, 4))
        };
    multiPoint = gf.createMultiPoint(points);
    testFilter(multiPoint);

    points = new Point[] {gf.createPoint(), gf.createPoint(), gf.createPoint()};
    multiPoint = gf.createMultiPoint(points);
    testFilter(multiPoint);

    multiPoint =
        gf.createMultiPointFromCoords(
            new Coordinate[] {
              new CoordinateXYM(1, 2, 3), new CoordinateXYM(4, 5, 6), new CoordinateXYM(7, 8, 9),
            });
    multiPoint.setSRID(4326);
    testFilter(multiPoint);
  }

  @Test
  public void testLineString() {
    testFilter(gf.createLineString());
    Coordinate[] coordinates =
        new Coordinate[] {
          new Coordinate(1.0, 2.0, 3.0), new Coordinate(4.0, 5.0, 6.0),
        };
    LineString lineString = gf.createLineString(coordinates);
    testFilter(lineString);
  }

  @Test
  public void testMultiLineString() {
    testFilter(gf.createMultiLineString());
    MultiLineString multiLineString =
        gf.createMultiLineString(
            new LineString[] {
              gf.createLineString(
                  new Coordinate[] {
                    new Coordinate(1, 2), new Coordinate(3, 4), new Coordinate(5, 6),
                  }),
              gf.createLineString(),
              gf.createLineString(
                  new Coordinate[] {
                    new Coordinate(7, 8), new Coordinate(9, 10), new Coordinate(11, 12),
                  }),
            });
    testFilter(multiLineString);

    multiLineString =
        gf.createMultiLineString(
            new LineString[] {gf.createLineString(), gf.createLineString(), gf.createLineString()});
    testFilter(multiLineString);
  }

  @Test
  public void testPolygon() {
    testFilter(gf.createPolygon());

    LinearRing shell =
        gf.createLinearRing(
            new Coordinate[] {
              new Coordinate(0, 0),
              new Coordinate(0, 1),
              new Coordinate(1, 1),
              new Coordinate(1, 0),
              new Coordinate(0, 0)
            });
    Polygon polygon = gf.createPolygon(shell);
    testFilter(polygon);

    shell =
        gf.createLinearRing(
            new Coordinate[] {
              new Coordinate(0, 0),
              new Coordinate(0, 1),
              new Coordinate(1, 1),
              new Coordinate(1, 0),
              new Coordinate(0, 0)
            });
    LinearRing hole1 =
        gf.createLinearRing(
            new Coordinate[] {
              new Coordinate(0.1, 0.1),
              new Coordinate(0.1, 0.2),
              new Coordinate(0.2, 0.2),
              new Coordinate(0.2, 0.1),
              new Coordinate(0.1, 0.1)
            });
    LinearRing hole2 =
        gf.createLinearRing(
            new Coordinate[] {
              new Coordinate(0.3, 0.3),
              new Coordinate(0.3, 0.4),
              new Coordinate(0.4, 0.4),
              new Coordinate(0.4, 0.3),
              new Coordinate(0.3, 0.3)
            });
    polygon = gf.createPolygon(shell, new LinearRing[] {hole1, hole2});
    testFilter(polygon);
  }

  @Test
  public void testMultiPolygon() {
    testFilter(gf.createMultiPolygon());

    LinearRing shell =
        gf.createLinearRing(
            new Coordinate[] {
              new Coordinate(0, 0),
              new Coordinate(0, 1),
              new Coordinate(1, 1),
              new Coordinate(1, 0),
              new Coordinate(0, 0)
            });
    LinearRing hole1 =
        gf.createLinearRing(
            new Coordinate[] {
              new Coordinate(0.1, 0.1),
              new Coordinate(0.1, 0.2),
              new Coordinate(0.2, 0.2),
              new Coordinate(0.2, 0.1),
              new Coordinate(0.1, 0.1)
            });
    LinearRing hole2 =
        gf.createLinearRing(
            new Coordinate[] {
              new Coordinate(0.3, 0.3),
              new Coordinate(0.3, 0.4),
              new Coordinate(0.4, 0.4),
              new Coordinate(0.4, 0.3),
              new Coordinate(0.3, 0.3)
            });
    LinearRing[] holes = new LinearRing[] {hole1, hole2};
    MultiPolygon multiPolygon =
        gf.createMultiPolygon(
            new Polygon[] {
              gf.createPolygon(shell), gf.createPolygon(), gf.createPolygon(shell, holes)
            });
    testFilter(multiPolygon);

    multiPolygon = gf.createMultiPolygon(new Polygon[] {gf.createPolygon(), gf.createPolygon()});
    testFilter(multiPolygon);
  }

  @Test
  public void testGeometryCollection() {
    testFilter(gf.createGeometryCollection());

    Point point = gf.createPoint(new Coordinate(10, 20));
    LineString lineString =
        gf.createLineString(new Coordinate[] {new Coordinate(0, 0), new Coordinate(1, 1)});
    Polygon polygon =
        gf.createPolygon(
            gf.createLinearRing(
                new Coordinate[] {
                  new Coordinate(0, 0),
                  new Coordinate(0, 1),
                  new Coordinate(1, 1),
                  new Coordinate(1, 0),
                  new Coordinate(0, 0)
                }),
            null);
    MultiPoint multiPoint =
        gf.createMultiPointFromCoords(
            new Coordinate[] {
              new Coordinate(10, 20), new Coordinate(30, 40), new Coordinate(50, 60)
            });
    MultiLineString multiLineString =
        gf.createMultiLineString(new LineString[] {lineString, lineString, lineString});
    MultiPolygon multiPolygon = gf.createMultiPolygon(new Polygon[] {polygon, polygon});
    GeometryCollection geometryCollection =
        gf.createGeometryCollection(
            new Geometry[] {
              gf.createPoint(),
              gf.createLineString(),
              gf.createPolygon(),
              point,
              lineString,
              polygon,
              gf.createMultiPoint(),
              gf.createMultiLineString(),
              gf.createMultiPolygon(),
              gf.createMultiPoint(new Point[] {gf.createPoint(), gf.createPoint()}),
              gf.createMultiLineString(
                  new LineString[] {gf.createLineString(), gf.createLineString()}),
              gf.createMultiPolygon(
                  new Polygon[] {gf.createPolygon(), gf.createPolygon(), gf.createPolygon()}),
              multiPoint,
              multiLineString,
              multiPolygon,
              point
            });
    geometryCollection.setSRID(4326);
    testFilter(geometryCollection);
  }

  @Test
  public void testNestedGeometryCollection() {
    Point point = gf.createPoint(new Coordinate(10, 20));
    LineString lineString =
        gf.createLineString(new Coordinate[] {new Coordinate(0, 0), new Coordinate(1, 1)});
    MultiLineString multiLineString =
        gf.createMultiLineString(new LineString[] {lineString, lineString, lineString});
    GeometryCollection geomCollection1 =
        gf.createGeometryCollection(new Geometry[] {point, lineString, multiLineString});
    GeometryCollection geomCollection2 =
        gf.createGeometryCollection(
            new Geometry[] {
              point, geomCollection1, geomCollection1, multiLineString, geomCollection1
            });
    geomCollection2.setSRID(4326);
    testFilter(geomCollection2);
  }
}
