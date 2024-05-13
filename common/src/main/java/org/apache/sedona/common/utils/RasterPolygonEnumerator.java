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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class RasterPolygonEnumerator {

    public static final int GP_NODATA_MARKER = -51502112;

    private static final Logger logger = LoggerFactory.getLogger(RasterPolygonEnumerator.class);

    public int[] polyIdMap = new int[0];
    public int[] polyValue = new int[0];

    private int nextPolygonId = 0;
    private int polyAlloc = 0;

    private int connectedness = 0;

    public RasterPolygonEnumerator(int connectedness) {
        assert connectedness == 4 || connectedness == 8;
        this.connectedness = connectedness;
    }

    public void clear() {
        polyIdMap = new int[0];
        polyValue = new int[0];

        nextPolygonId = 0;
        polyAlloc = 0;
    }

    /**
     * Update the polygon map to indicate the merger of two polygons.
     */
    private void mergePolygon(int srcId, int dstIdInit) {
        // Figure out the final dest id.
        int dstIdFinal = dstIdInit;
        while (polyIdMap[dstIdFinal] != dstIdFinal)
            dstIdFinal = polyIdMap[dstIdFinal];

        // Map the whole intermediate chain to it.
        int dstIdCur = dstIdInit;
        while (polyIdMap[dstIdCur] != dstIdCur) {
            int nextDstId = polyIdMap[dstIdCur];
            polyIdMap[dstIdCur] = dstIdFinal;
            dstIdCur = nextDstId;
        }

        // And map the whole source chain to it too (can be done in one pass).
        while (polyIdMap[srcId] != srcId) {
            int nextSrcId = polyIdMap[srcId];
            polyIdMap[srcId] = dstIdFinal;
            srcId = nextSrcId;
        }
        polyIdMap[srcId] = dstIdFinal;
    }

    /**
     * Allocate a new polygon id, and reallocate the polygon maps if needed
     */
    private int newPolygon(int value) {
        if (nextPolygonId == Integer.MAX_VALUE) {
            throw new IllegalArgumentException("maximum number of polygons reached");
        }
        if (nextPolygonId >= polyAlloc) {
            int polyAllocNew;
            if (polyAlloc < (Integer.MAX_VALUE - 20) / 2)
                polyAllocNew = polyAlloc * 2 + 20;
            else
                polyAllocNew = Integer.MAX_VALUE;
            if (polyAllocNew > Integer.MAX_VALUE / 4) {
                throw new IllegalArgumentException("too many polygons");
            }

            polyIdMap = Arrays.copyOf(polyIdMap, polyAllocNew);
            polyValue = Arrays.copyOf(polyValue, polyAllocNew);
            polyAlloc = polyAllocNew;
        }

        int polyId = nextPolygonId;
        polyIdMap[polyId] = polyId;
        polyValue[polyId] = value;
        nextPolygonId++;

        return polyId;
    }

    /**
     * Make a pass through the maps, ensuring every polygon id points to the final id it should use,
     * not an intermediate value.
     */
    public void completeMerges() {
        int finalPolyCount = 0;

        for (int iPoly = 0; iPoly < nextPolygonId; iPoly++) {
            // Figure out the final id.
            int id = polyIdMap[iPoly];
            while (id != polyIdMap[id]) {
                id = polyIdMap[id];
            }

            // Then map the whole intermediate chain to it.
            int idCur = polyIdMap[iPoly];
            polyIdMap[iPoly] = id;
            while (idCur != polyIdMap[idCur]) {
                int nextId = polyIdMap[idCur];
                polyIdMap[idCur] = id;
                idCur = nextId;
            }

            if (polyIdMap[iPoly] == iPoly) {
                finalPolyCount++;
            }
        }

        logger.debug("Counted {} polygon fragments forming {} final polygons.", nextPolygonId, finalPolyCount);
    }

    /**
     * Assign ids to polygons, one line at a time.
     */
    public boolean processLine(int[] lastLineVal, int[] thisLineVal, int[] lastLineId, int[] thisLineId) {
        int nXSize = thisLineVal.length;

        /* -------------------------------------------------------------------- */
        /*      Special case for the first line.                                */
        /* -------------------------------------------------------------------- */
        if (lastLineVal == null) {
            for (int i = 0; i < nXSize; i++) {
                if (thisLineVal[i] == GP_NODATA_MARKER) {
                    thisLineId[i] = -1;
                } else if (i == 0 || thisLineVal[i] != thisLineVal[i - 1]) {
                    thisLineId[i] = newPolygon(thisLineVal[i]);
                    if (thisLineId[i] < 0) {
                        return false;
                    }
                } else {
                    thisLineId[i] = thisLineId[i - 1];
                }
            }
            return true;
        }

        /* -------------------------------------------------------------------- */
        /*      Process each pixel comparing to the previous pixel, and to      */
        /*      the last line.                                                  */
        /* -------------------------------------------------------------------- */
        for (int i = 0; i < nXSize; i++) {
            if (thisLineVal[i] == GP_NODATA_MARKER) {
                thisLineId[i] = -1;
            } else if (i > 0 && thisLineVal[i] == thisLineVal[i - 1]) {
                thisLineId[i] = thisLineId[i - 1];

                if (lastLineVal[i] == thisLineVal[i] &&
                        polyIdMap[lastLineId[i]] != polyIdMap[thisLineId[i]]) {
                    mergePolygon(lastLineId[i], thisLineId[i]);
                }

                if (connectedness == 8 &&
                        lastLineVal[i - 1] == thisLineVal[i] &&
                        polyIdMap[lastLineId[i - 1]] != polyIdMap[thisLineId[i]]) {
                    mergePolygon(lastLineId[i - 1], thisLineId[i]);
                }

                if (connectedness == 8 && i < nXSize - 1 &&
                        lastLineVal[i + 1] == thisLineVal[i] &&
                        polyIdMap[lastLineId[i + 1]] != polyIdMap[thisLineId[i]]) {
                    mergePolygon(lastLineId[i + 1], thisLineId[i]);
                }
            } else if (lastLineVal[i] == thisLineVal[i]) {
                thisLineId[i] = lastLineId[i];
            } else if (i > 0 && connectedness == 8 &&
                    lastLineVal[i - 1] == thisLineVal[i]) {
                thisLineId[i] = lastLineId[i - 1];

                if (i < nXSize - 1 && lastLineVal[i + 1] == thisLineVal[i] &&
                        polyIdMap[lastLineId[i + 1]] != polyIdMap[thisLineId[i]]) {
                    mergePolygon(lastLineId[i + 1], thisLineId[i]);
                }
            } else if (i < nXSize - 1 && connectedness == 8 &&
                    lastLineVal[i + 1] == thisLineVal[i]) {
                thisLineId[i] = lastLineId[i + 1];
            } else {
                thisLineId[i] = newPolygon(thisLineVal[i]);
                if (thisLineId[i] < 0) {
                    return false;
                }
            }
        }
        return true;
    }
}
