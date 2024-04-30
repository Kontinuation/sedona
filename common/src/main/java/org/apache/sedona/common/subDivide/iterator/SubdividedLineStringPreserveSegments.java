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

import org.apache.sedona.common.subDivide.SubdivideOptions;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Subdivide a line string into smaller line strings of a given maximum length. We don't cut in the middle of
 * segments of the line string, but only split the line string at the vertices. The subdivided line strings could
 * be slightly larger than the maximum length.
 */
public class SubdividedLineStringPreserveSegments implements Iterator<Geometry> {
    private final GeometryFactory factory;
    private final Coordinate[] coordinates;
    private final double maxLength;
    private int index = 0;

    public SubdividedLineStringPreserveSegments(LineString lineString, SubdivideOptions options) {
        factory = lineString.getFactory();
        maxLength = (options.maxWidth * 0.5 + options.maxHeight * 0.5);
        coordinates = lineString.getCoordinates();
        if (coordinates.length == 1) {
            throw new IllegalArgumentException("non-empty LineString must have at least two coordinates");
        }
    }

    public boolean hasNext() {
        return index < coordinates.length - 1;
    }

    public Geometry next() {
        double length = 0;
        List<Coordinate> slice = new ArrayList<>();
        Coordinate prev = coordinates[index++];
        slice.add(prev);
        for ( ; index < coordinates.length; index++) {
            Coordinate current = coordinates[index];
            slice.add(current);
            length += current.distance(prev);
            if (length >= maxLength) {
                break;
            } else {
                prev = current;
            }
        }
        return factory.createLineString(slice.toArray(new Coordinate[0]));
    }
}
