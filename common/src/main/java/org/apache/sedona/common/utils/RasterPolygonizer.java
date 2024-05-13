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
package org.apache.sedona.common.utils;

import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This raster polygonizer code is mostly taken from GDALPolygonizeT in GDAL library 3.8.5, which implements
 * a two-arm algorithm for raster polygonization.
 */
public class RasterPolygonizer {

    public static class PolygonWithValue {
        public Polygon polygon;
        public int value;

        public PolygonWithValue(Polygon polygon, int value) {
            this.polygon = polygon;
            this.value = value;
        }
    }

    public static List<PolygonWithValue> polygonize(int[] grid, int width, int nConnectedness, AffineTransform2D affine) {
        if (nConnectedness != 4 && nConnectedness != 8) {
            throw new IllegalArgumentException("Connectedness must be 4 or 8");
        }
        int nXSize = width;
        int nYSize = grid.length / width;

        /* -------------------------------------------------------------------- */
        /*      Allocate working buffers.                                       */
        /* -------------------------------------------------------------------- */
        int[] thisLineVal = new int[nXSize];
        int[] lastLineVal = new int[nXSize];
        int[] thisLineId = new int[nXSize];
        int[] lastLineId = new int[nXSize];

        /* -------------------------------------------------------------------- */
        /*      The first pass over the raster is only used to build up the     */
        /*      polygon id map so we will know in advance what polygons are     */
        /*      what on the second pass.                                        */
        /* -------------------------------------------------------------------- */
        RasterPolygonEnumerator firstEnum = new RasterPolygonEnumerator(nConnectedness);
        for (int iY = 0; iY < nYSize; iY++) {
            System.arraycopy(grid, iY * nXSize, thisLineVal, 0, nXSize);
            if (iY == 0) {
                firstEnum.processLine(null, thisLineVal, null, thisLineId);
            } else {
                firstEnum.processLine(lastLineVal, thisLineVal, lastLineId, thisLineId);
            }

            // Swap lines
            int[] tmpLineVal = lastLineVal;
            lastLineVal = thisLineVal;
            thisLineVal = tmpLineVal;
            int[] tmpLineId = lastLineId;
            lastLineId = thisLineId;
            thisLineId = tmpLineId;
        }

        /* -------------------------------------------------------------------- */
        /*      Make a pass through the maps, ensuring every polygon id         */
        /*      points to the final id it should use, not an intermediate       */
        /*      value.                                                          */
        /* -------------------------------------------------------------------- */
        firstEnum.completeMerges();

        /* -------------------------------------------------------------------- */
        /*      We will use a new enumerator for the second pass primarily      */
        /*      so we can preserve the first pass map.                          */
        /* -------------------------------------------------------------------- */
        RasterPolygonEnumerator secondEnum = new RasterPolygonEnumerator(nConnectedness);

        double[] geoTransform = new double[6];
        geoTransform[0] = affine.getTranslateX();
        geoTransform[1] = affine.getScaleX();
        geoTransform[2] = affine.getShearX();
        geoTransform[3] = affine.getTranslateY();
        geoTransform[4] = affine.getShearY();
        geoTransform[5] = affine.getScaleY();

        RasterPolygonizer.PolygonWriter polygonWriter = new RasterPolygonizer.PolygonWriter(geoTransform);
        RasterPolygonizer.Polygonizer polygonizer = new RasterPolygonizer.Polygonizer(-1, polygonWriter);
        RasterPolygonizer.TwoArm[] lastLineArm = new RasterPolygonizer.TwoArm[nXSize + 2];
        RasterPolygonizer.TwoArm[] thisLineArm = new RasterPolygonizer.TwoArm[nXSize + 2];
        for (int i = 0; i < nXSize + 2; i++) {
            lastLineArm[i] = new RasterPolygonizer.TwoArm();
            thisLineArm[i] = new RasterPolygonizer.TwoArm();
            lastLineArm[i].polyInside = polygonizer.getTheOuterPolygon();
        }

        /* ==================================================================== */
        /*      Second pass during which we will actually collect polygon       */
        /*      edges as geometries.                                            */
        /* ==================================================================== */
        for (int iY = 0; iY < nYSize + 1; iY++) {
            /* -----------------------------*/
            /*      Read the image data.    */
            /* -----------------------------*/
            if (iY < nYSize) {
                System.arraycopy(grid, iY * nXSize, thisLineVal, 0, nXSize);
            }

            /* -------------------------------------------------------------------*/
            /*      Determine what polygon the various pixels belong to (redoing  */
            /*      the same thing done in the first pass above).                 */
            /* -------------------------------------------------------------------*/
            if (iY == nYSize) {
                for (int iX = 0; iX < nXSize; iX++) {
                    thisLineId[iX] = RasterPolygonizer.Polygonizer.THE_OUTER_POLYGON_ID;
                }
            } else if (iY == 0) {
                secondEnum.processLine(null, thisLineVal, null, thisLineId);
            } else {
                secondEnum.processLine(lastLineVal, thisLineVal, lastLineId, thisLineId);
            }

            if (iY < nYSize) {
                for (int iX = 0; iX < nXSize; iX++) {
                    // TODO: maybe we can reserve -1 as the lookup result for -1 polygon id in the panPolyIdMap,
                    //       so the this expression becomes: panLastLineId[iX] = *(oFirstEnum.panPolyIdMap + panThisLineId[iX]).
                    //       This would eliminate the condition checking.
                    lastLineId[iX] = (thisLineId[iX] == -1? -1: firstEnum.polyIdMap[thisLineId[iX]]);
                }
                polygonizer.processLine(lastLineId, lastLineVal, thisLineArm, lastLineArm, iY, nXSize);
            } else {
                polygonizer.processLine(thisLineId, lastLineVal, thisLineArm, lastLineArm, iY, nXSize);
            }

            /* --------------------------------------------------------------------*/
            /*      Swap pixel value, and polygon id lines to be ready for the     */
            /*      next line.                                                     */
            /* --------------------------------------------------------------------*/
            int[] tmpLineVal = lastLineVal;
            lastLineVal = thisLineVal;
            thisLineVal = tmpLineVal;
            int[] tmpLineId = lastLineId;
            lastLineId = thisLineId;
            thisLineId = tmpLineId;
            RasterPolygonizer.TwoArm[] tmpArm = lastLineArm;
            lastLineArm = thisLineArm;
            thisLineArm = tmpArm;
        }

        return polygonWriter.getPolygonsWithValues();
    }

    static class Point {
        int row;
        int col;
        Point(int row, int col) {
            this.row = row;
            this.col = col;
        }
    }

    static class Arc {
        ArrayList<Point> points = new ArrayList<>();
        void add(Point point) {
            points.add(point);
        }
    }

    static class IndexedArc {
        Arc arc;
        int index;
        IndexedArc(Arc arc, int index) {
            this.arc = arc;
            this.index = index;
        }
        IndexedArc() {
            this(null, 0);
        }
    }

    static class RPolygon {
        int bottomRightRow = 0;
        int bottomRightCol = 0;
        final ArrayList<Arc> arcs = new ArrayList<>();
        final ArrayList<Boolean> arcRightHandFollow = new ArrayList<>();
        final ArrayList<Integer> arcConnections = new ArrayList<>();

        IndexedArc newArc(boolean followRightHand) {
            Arc arc = new Arc();
            int arcIndex = arcs.size();
            arcs.add(arc);
            arcRightHandFollow.add(followRightHand);
            arcConnections.add(arcIndex);
            return new IndexedArc(arc, arcIndex);
        }

        void setArcConnection(IndexedArc arc, IndexedArc nextArc) {
            arcConnections.set(arc.index, nextArc.index);
        }

        void updateBottomRightPos(int row, int col) {
            bottomRightRow = row;
            bottomRightCol = col;
        }
    }

    static class TwoArm {
        int row = 0;
        int col = 0;

        RPolygon polyInside = null;
        RPolygon polyAbove = null;
        RPolygon polyLeft = null;

        IndexedArc arcHorOuter = new IndexedArc();
        IndexedArc arcHorInner = new IndexedArc();
        IndexedArc arcVerOuter = new IndexedArc();
        IndexedArc arcVerInner = new IndexedArc();

        boolean solidHorizontal = false;
        boolean solidVertical = false;
    }

    interface PolygonReceiver {
        void receive(RPolygon polygon, int value);
    }

    static class Polygonizer {
        static final int THE_OUTER_POLYGON_ID = Integer.MAX_VALUE;

        final int invalidPolyId;
        final RPolygon theOuterPolygon;
        final Map<Integer, RPolygon> polygonMap = new HashMap<>();
        final PolygonReceiver polygonReceiver;

        Polygonizer(int invalidPolyId, PolygonReceiver polygonReceiver) {
            this.invalidPolyId = invalidPolyId;
            this.polygonReceiver = polygonReceiver;
            theOuterPolygon = createPolygon(THE_OUTER_POLYGON_ID);
        }

        RPolygon getPolygon(int polygonId) {
            RPolygon polygon = polygonMap.get(polygonId);
            if (polygon == null) {
                polygon = createPolygon(polygonId);
            }
            return polygon;
        }

        RPolygon createPolygon(int polygonId) {
            RPolygon polygon = new RPolygon();
            polygonMap.put(polygonId, polygon);
            return polygon;
        }

        void destroyPolygon(int polygonId) {
            polygonMap.remove(polygonId);
        }

        RPolygon getTheOuterPolygon() {
            return theOuterPolygon;
        }

        void processLine(int[] thisLineId, int[] lastLineVal, TwoArm[] thisLineArm, TwoArm[] lastLineArm,
                         int nCurrentRow, int nCols) {
            TwoArm current = thisLineArm[1];
            current.row = nCurrentRow;
            current.col = 0;
            current.polyInside = getPolygon(thisLineId[0]);
            TwoArm above = lastLineArm[1];
            TwoArm left = thisLineArm[0];
            left.polyInside = theOuterPolygon;
            ProcessArmConnections(current, above, left);
            for (int col = 1; col < nCols; ++col)
            {
                int iArmIndex = col + 1;
                current = thisLineArm[iArmIndex];
                current.row = nCurrentRow;
                current.col = col;
                current.polyInside = getPolygon(thisLineId[col]);
                above = lastLineArm[iArmIndex];
                left = thisLineArm[iArmIndex - 1];
                ProcessArmConnections(current, above, left);
            }
            current = thisLineArm[nCols + 1];
            current.row = nCurrentRow;
            current.col = nCols;
            current.polyInside = theOuterPolygon;
            above = lastLineArm[nCols + 1];
            above.polyInside = theOuterPolygon;
            left = thisLineArm[nCols];
            ProcessArmConnections(current, above, left);

            /*
             * Find those polygons haven't been processed on this line as we can be sure they are completed
             */
            ArrayList<Map.Entry<Integer, RPolygon>> completedPolygons = new ArrayList<>();
            for (Map.Entry<Integer, RPolygon> entry : polygonMap.entrySet()) {
                RPolygon polygon = entry.getValue();

                if (polygon.bottomRightRow + 1 == nCurrentRow)
                {
                    completedPolygons.add(entry);
                }
            }
            for (Map.Entry<Integer, RPolygon> entry : completedPolygons) {
                int polyId = entry.getKey();
                RPolygon polygon = entry.getValue();

                // emit valid polygon only
                if (polyId != invalidPolyId) {
                    polygonReceiver.receive(polygon, lastLineVal[polygon.bottomRightCol]);
                }

                destroyPolygon(polyId);
            }
        }
    }

    /**
     * Process different kinds of Arm connections.
     */
    static void ProcessArmConnections(TwoArm current, TwoArm above, TwoArm left) {
        current.polyInside.updateBottomRightPos(current.row, current.col);
        current.solidVertical = current.polyInside != left.polyInside;
        current.solidHorizontal = current.polyInside != above.polyInside;
        current.polyAbove = above.polyInside;
        current.polyLeft = left.polyInside;

        final int BIT_CUR_HORIZ = 0;
        final int BIT_CUR_VERT = 1;
        final int BIT_LEFT = 2;
        final int BIT_ABOVE = 3;

        int nArmConnectionType = ((above.solidVertical ? 1 : 0) << BIT_ABOVE) |
                ((left.solidHorizontal ? 1 : 0) << BIT_LEFT) |
                ((current.solidVertical ? 1 : 0) << BIT_CUR_VERT) |
                ((current.solidHorizontal ? 1 : 0) << BIT_CUR_HORIZ);

        final int VIRTUAL = 0;
        final int SOLID = 1;

        final int ABOVE_VIRTUAL = VIRTUAL << BIT_ABOVE;
        final int ABOVE_SOLID = SOLID << BIT_ABOVE;

        final int LEFT_VIRTUAL = VIRTUAL << BIT_LEFT;
        final int LEFT_SOLID = SOLID << BIT_LEFT;

        final int CUR_VERT_VIRTUAL = VIRTUAL << BIT_CUR_VERT;
        final int CUR_VERT_SOLID = SOLID << BIT_CUR_VERT;

        final int CUR_HORIZ_VIRTUAL = VIRTUAL << BIT_CUR_HORIZ;
        final int CUR_HORIZ_SOLID = SOLID << BIT_CUR_HORIZ;

        /*
         * There are 12 valid connection types depending on the arm types(virtual or solid)
         * The following diagram illustrates these kinds of connection types, ⇢⇣ means virtual arm, →↓ means solid arm.
         *     ⇣        ⇣          ⇣         ⇣        ↓
         *    ⇢ →      → →        → ⇢       → →      ⇢ →
         *     ↓        ⇣          ↓         ↓        ⇣
         *   type=3    type=5    type=6    type=7    type=9
         *
         *     ↓        ↓          ↓         ↓          ↓
         *    ⇢ ⇢      ⇢ →        → ⇢       → →        → ⇢
         *     ↓        ↓          ⇣         ⇣          ↓
         *   type=10  type=11    type=12    type=13   type=14
         *
         *     ↓        ⇣
         *    → →      ⇢ ⇢
         *     ↓        ⇣
         *   type=15  type=0
         *
         *   For each connection type, we may create new arc, ,
         *   Depending on the connection type, we may do the following things:
         *       1. Create new arc. If the arc is closed to the inner polygon, it is called "Inner Arc", otherwise "Outer Arc"
         *       2. Pass an arc to the next arm.
         *       3. "Close" two arcs. If two arcs meet at the bottom right corner of a cell, close them by recording the arc connection.
         *       4. Add grid position(row, col) to an arc.
         */
        switch (nArmConnectionType) {
            case ABOVE_VIRTUAL | LEFT_VIRTUAL | CUR_VERT_VIRTUAL | CUR_HORIZ_VIRTUAL:  // 0
                // nothing to do
                break;

            case ABOVE_VIRTUAL | LEFT_VIRTUAL | CUR_VERT_SOLID | CUR_HORIZ_SOLID:  // 3
                // add inner arcs
                current.arcVerInner = current.polyInside.newArc(true);
                current.arcHorInner = current.polyInside.newArc(false);
                current.polyInside.setArcConnection(current.arcHorInner, current.arcVerInner);
                current.arcVerInner.arc.add(new Point(current.row, current.col));

                // add outer arcs
                current.arcHorOuter = above.polyInside.newArc(true);
                current.arcVerOuter = above.polyInside.newArc(false);
                above.polyInside.setArcConnection(current.arcVerOuter, current.arcHorOuter);
                current.arcHorOuter.arc.add(new Point(current.row, current.col));

                break;

            case ABOVE_VIRTUAL | LEFT_SOLID | CUR_VERT_VIRTUAL | CUR_HORIZ_SOLID:  // 5
                // pass arcs
                current.arcHorInner = left.arcHorInner;
                current.arcHorOuter = left.arcHorOuter;
                break;

            case ABOVE_VIRTUAL | LEFT_SOLID | CUR_VERT_SOLID | CUR_HORIZ_VIRTUAL:  // 6
                // pass arcs
                current.arcVerInner = left.arcHorOuter;
                current.arcVerOuter = left.arcHorInner;
                current.arcVerInner.arc.add(new Point(current.row, current.col));
                current.arcVerOuter.arc.add(new Point(current.row, current.col));

                break;

            case ABOVE_VIRTUAL | LEFT_SOLID | CUR_VERT_SOLID | CUR_HORIZ_SOLID:  // 7
                // pass arcs
                current.arcHorOuter = left.arcHorOuter;
                current.arcVerOuter = left.arcHorInner;
                left.arcHorInner.arc.add(new Point(current.row, current.col));

                // add inner arcs
                current.arcVerInner = current.polyInside.newArc(true);
                current.arcHorInner = current.polyInside.newArc(false);
                current.polyInside.setArcConnection(current.arcHorInner, current.arcVerInner);
                current.arcVerInner.arc.add(new Point(current.row, current.col));

                break;

            case ABOVE_SOLID | LEFT_VIRTUAL | CUR_VERT_VIRTUAL | CUR_HORIZ_SOLID:  // 9
                // pass arcs
                current.arcHorOuter = above.arcVerInner;
                current.arcHorInner = above.arcVerOuter;
                current.arcHorOuter.arc.add(new Point(current.row, current.col));
                current.arcHorInner.arc.add(new Point(current.row, current.col));

                break;

            case ABOVE_SOLID | LEFT_VIRTUAL | CUR_VERT_SOLID | CUR_HORIZ_VIRTUAL:  // 10
                // pass arcs
                current.arcVerInner = above.arcVerInner;
                current.arcVerOuter = above.arcVerOuter;

                break;
            case ABOVE_SOLID | LEFT_VIRTUAL | CUR_VERT_SOLID | CUR_HORIZ_SOLID:  // 11
                // pass arcs
                current.arcHorOuter = above.arcVerInner;
                current.arcVerOuter = above.arcVerOuter;
                current.arcHorOuter.arc.add(new Point(current.row, current.col));
                // add inner arcs
                current.arcVerInner = current.polyInside.newArc(true);
                current.arcHorInner = current.polyInside.newArc(false);
                current.polyInside.setArcConnection(current.arcHorInner, current.arcVerInner);
                current.arcVerInner.arc.add(new Point(current.row, current.col));

                break;
            case ABOVE_SOLID | LEFT_SOLID | CUR_VERT_VIRTUAL | CUR_HORIZ_VIRTUAL:  // 12
                // close arcs
                left.arcHorOuter.arc.add(new Point(current.row, current.col));
                left.polyAbove.setArcConnection(left.arcHorOuter, above.arcVerOuter);
                // close arcs
                above.arcVerInner.arc.add(new Point(current.row, current.col));
                current.polyInside.setArcConnection(above.arcVerInner, left.arcHorInner);

                break;
            case ABOVE_SOLID | LEFT_SOLID | CUR_VERT_VIRTUAL | CUR_HORIZ_SOLID:  // 13
                // close arcs
                left.arcHorOuter.arc.add(new Point(current.row, current.col));
                left.polyAbove.setArcConnection(left.arcHorOuter, above.arcVerOuter);
                // pass arcs
                current.arcHorOuter = above.arcVerInner;
                current.arcHorInner = left.arcHorInner;
                current.arcHorOuter.arc.add(new Point(current.row, current.col));

                break;
            case ABOVE_SOLID | LEFT_SOLID | CUR_VERT_SOLID | CUR_HORIZ_VIRTUAL:  // 14
                // close arcs
                left.arcHorOuter.arc.add(new Point(current.row, current.col));
                left.polyAbove.setArcConnection(left.arcHorOuter, above.arcVerOuter);
                // pass arcs
                current.arcVerInner = above.arcVerInner;
                current.arcVerOuter = left.arcHorInner;
                current.arcVerOuter.arc.add(new Point(current.row, current.col));

                break;

            case ABOVE_SOLID | LEFT_SOLID | CUR_VERT_SOLID | CUR_HORIZ_SOLID:  // 15
                // Two pixels of the main diagonal belong to the same polygon
                if (above.polyLeft == current.polyInside) {
                    // pass arcs
                    current.arcVerInner = left.arcHorOuter;
                    current.arcHorInner = above.arcVerOuter;
                    current.arcVerInner.arc.add(new Point(current.row, current.col));
                    current.arcHorInner.arc.add(new Point(current.row, current.col));
                } else {
                    // close arcs
                    left.arcHorOuter.arc.add(new Point(current.row, current.col));
                    left.polyAbove.setArcConnection(left.arcHorOuter, above.arcVerOuter);
                    // add inner arcs
                    current.arcVerInner = current.polyInside.newArc(true);
                    current.arcHorInner = current.polyInside.newArc(false);
                    current.polyInside.setArcConnection(current.arcHorInner, current.arcVerInner);
                    current.arcVerInner.arc.add(new Point(current.row, current.col));
                }

                // Two pixels of the secondary diagonal belong to the same polygon
                if (above.polyInside == left.polyInside) {
                    // close arcs
                    above.polyInside.setArcConnection(above.arcVerInner, left.arcHorInner);
                    above.arcVerInner.arc.add(new Point(current.row, current.col));
                    // add outer arcs
                    current.arcHorOuter = above.polyInside.newArc(true);
                    current.arcVerOuter = above.polyInside.newArc(false);
                    current.arcHorOuter.arc.add(new Point(current.row, current.col));
                    above.polyInside.setArcConnection(current.arcVerOuter, current.arcHorOuter);
                } else {
                    // pass arcs
                    current.arcHorOuter = above.arcVerInner;
                    current.arcVerOuter = left.arcHorInner;
                    current.arcHorOuter.arc.add(new Point(current.row, current.col));
                    current.arcVerOuter.arc.add(new Point(current.row, current.col));
                }

                break;

            case ABOVE_VIRTUAL | LEFT_VIRTUAL | CUR_VERT_VIRTUAL | CUR_HORIZ_SOLID:  // 1
            case ABOVE_VIRTUAL | LEFT_VIRTUAL | CUR_VERT_SOLID | CUR_HORIZ_VIRTUAL:  // 2
            case ABOVE_VIRTUAL | LEFT_SOLID | CUR_VERT_VIRTUAL | CUR_HORIZ_VIRTUAL:  // 4
            default:
                // Impossible case
                throw new AssertionError("Impossible case");
        }
    }

    static class PolygonWriter implements PolygonReceiver {
        final double[] geoTransform;
        final GeometryFactory factory = new GeometryFactory();
        final ArrayList<PolygonWithValue> polygonsWithValues = new ArrayList<>();

        ArrayList<PolygonWithValue> getPolygonsWithValues() {
            return polygonsWithValues;
        }

        PolygonWriter(double[] geoTransform) {
            this.geoTransform = geoTransform;
        }

        @Override
        public void receive(RPolygon polygon, int value) {
            boolean[] accessedArc = new boolean[polygon.arcConnections.size()];
            Arrays.fill(accessedArc, false);

            ArrayList<Coordinate[]> rings = new ArrayList<>();

            for (int firstArcIndex = 0; firstArcIndex < polygon.arcConnections.size(); firstArcIndex++) {
                if (accessedArc[firstArcIndex]) continue;

                ArrayList<Coordinate> ring = new ArrayList<>();
                addArcToRing(polygon, ring, geoTransform, firstArcIndex, accessedArc);
                // close ring manually
                ring.add(ring.get(0).copy());
                rings.add(ring.toArray(new Coordinate[0]));
            }

            // The first ring should be the shell, and the rest are holes
            LinearRing shell = factory.createLinearRing(rings.get(0));
            LinearRing[] holes = new LinearRing[rings.size() - 1];
            for (int i = 1; i < rings.size(); i++) {
                holes[i - 1] = factory.createLinearRing(rings.get(i));
            }
            Polygon jtsPolygon = factory.createPolygon(shell, holes);
            polygonsWithValues.add(new PolygonWithValue(jtsPolygon, value));
        }

        void addArcToRing(RPolygon polygon, ArrayList<Coordinate> ring, double[] geoTransform,
                                  int firstArcIndex, boolean[] accessedArc) {
            int iArcIndex = firstArcIndex;
            do {
                Arc arc = polygon.arcs.get(iArcIndex);
                boolean arcFollowRightHand = polygon.arcRightHandFollow.get(iArcIndex);
                for (int i = 0; i < arc.points.size(); ++i) {
                    Point oPixel = arc.points.get(arcFollowRightHand ? i : (arc.points.size() - i - 1));

                    double dfX = geoTransform[0] +
                            oPixel.col * geoTransform[1] +
                            oPixel.row * geoTransform[2];
                    double dfY = geoTransform[3] +
                            oPixel.col * geoTransform[4] +
                            oPixel.row * geoTransform[5];

                    ring.add(new Coordinate(dfX, dfY));
                }
                accessedArc[iArcIndex] = true;
                iArcIndex = polygon.arcConnections.get(iArcIndex);
            } while (iArcIndex != firstArcIndex);
        }

    }
}
