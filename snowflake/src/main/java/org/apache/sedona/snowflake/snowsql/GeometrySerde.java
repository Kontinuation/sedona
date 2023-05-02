package org.apache.sedona.snowflake.snowsql;


import org.apache.sedona.common.Functions;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

import java.util.Arrays;
import java.util.Base64;

public class GeometrySerde {

    public static byte[] serialize(Geometry geom) {
        return Functions.asEWKB(geom);
    }

    public static String bytesToString(byte[] geom) {
        return Base64.getEncoder().encodeToString(geom);
    }

    public static byte[] stringToBytes(String geom) {
        return Base64.getDecoder().decode(geom);
    }

    public static String[] serialize(Geometry[] geoms) {
        return Arrays.stream(geoms).map(geom -> bytesToString(GeometrySerde.serialize(geom))).toArray(String[]::new);
    }

    public static Geometry deserialize(byte[] bytes) {
        try {
            return new WKBReader().read(bytes);
        } catch (ParseException e) {
            String msg= String.format("Failed to parse WKB(printed through Arrays.toString(bytes)): %s, error: %s", Arrays.toString(bytes), e.getMessage());
            throw new IllegalArgumentException(msg);
        }
    }

    public static Geometry[] deserialize(String[] bytesStrings) {
        return Arrays.stream(bytesStrings).map(bytesStr -> GeometrySerde.deserialize(stringToBytes(bytesStr))).toArray(Geometry[]::new);
    }
}
