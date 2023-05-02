package org.apache.sedona.snowflake.snowsql;

import org.apache.sedona.common.Functions;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

public class GeoToolsWrapper {
    public static Geometry transform(Geometry geometry, String sourceCRS, String targetCRS, boolean lenient) {
        try {
            return Functions.transform(
                    geometry,
                    sourceCRS,
                    targetCRS,
                    lenient
            );
        } catch (FactoryException | TransformException e) {
            throw new RuntimeException(e);
        }
    }

    public static Geometry transform(Geometry geometry, String sourceCRS, String targetCRS) {
        try {
            return Functions.transform(
                    geometry,
                    sourceCRS,
                    targetCRS
            );
        } catch (FactoryException | TransformException e) {
            throw new RuntimeException(e);
        }
    }
}
