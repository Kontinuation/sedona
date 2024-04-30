/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sedona.common.subDivide.iterator;

import org.apache.sedona.common.subDivide.GeometrySubDivider;
import org.apache.sedona.common.subDivide.SubdivideOptions;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;

import java.util.Iterator;

public class SubdividedPolygonUsingOverlay implements Iterator<Geometry> {
    private final Geometry[] subdivided;
    private int index = 0;

    public SubdividedPolygonUsingOverlay(Polygon polygon, SubdivideOptions options) {
        subdivided = GeometrySubDivider.subDivide(polygon, options.maxCoordinates,
                GeometrySubDivider.OverlayAlgorithm.OverlayOld);
    }

    @Override
    public boolean hasNext() {
        return index < subdivided.length;
    }

    @Override
    public Geometry next() {
        return subdivided[index++];
    }
}
