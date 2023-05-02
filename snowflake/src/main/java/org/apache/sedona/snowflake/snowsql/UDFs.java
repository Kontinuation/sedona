package org.apache.sedona.snowflake.snowsql;

import org.apache.sedona.common.Constructors;
import org.apache.sedona.common.Functions;
import org.apache.sedona.common.Predicates;
import org.apache.sedona.common.enums.FileDataSplitter;
import org.apache.sedona.snowflake.snowsql.annotations.UDFAnnotations;
import org.locationtech.jts.io.ParseException;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

public class UDFs {

    @UDFAnnotations.ParamMeta(argNames = {"linestring", "point", "position"})
    public static byte[] ST_AddPoint(byte[] linestring, byte[] point, int position) {
        return GeometrySerde.serialize(
                Functions.addPoint(
                        GeometrySerde.deserialize(linestring),
                        GeometrySerde.deserialize(point),
                        position
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static double ST_Area(byte[] geometry) {
        return Functions.area(
                GeometrySerde.deserialize(geometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static byte[] ST_AsBinary(byte[] geometry) {
        return Functions.asWKB(
                GeometrySerde.deserialize(geometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static byte[] ST_AsEWKB(byte[] geometry) {
        return Functions.asEWKB(
                GeometrySerde.deserialize(geometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static String ST_AsEWKT(byte[] geometry) {
        return Functions.asEWKT(
                GeometrySerde.deserialize(geometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static String ST_AsGML(byte[] geometry) {
        return Functions.asGML(
                GeometrySerde.deserialize(geometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static String ST_AsGeoJSON(byte[] geometry) {
        return Functions.asGeoJson(
                GeometrySerde.deserialize(geometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static String ST_AsKML(byte[] geometry) {
        return Functions.asKML(
                GeometrySerde.deserialize(geometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"left", "right"})
    public static double ST_Azimuth(byte[] left, byte[] right) {
        return Functions.azimuth(
                GeometrySerde.deserialize(left),
                GeometrySerde.deserialize(right)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static byte[] ST_Boundary(byte[] geometry) {
        return GeometrySerde.serialize(
                Functions.boundary(
                        GeometrySerde.deserialize(geometry)
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry", "radius"})
    public static byte[] ST_Buffer(byte[] geometry, double radius) {
        return GeometrySerde.serialize(
                Functions.buffer(
                        GeometrySerde.deserialize(geometry),
                        radius
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static byte[] ST_BuildArea(byte[] geometry) {
        return GeometrySerde.serialize(
                Functions.buildArea(
                        GeometrySerde.deserialize(geometry)
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static byte[] ST_Centroid(byte[] geometry) {
        return GeometrySerde.serialize(
                Functions.getCentroid(
                        GeometrySerde.deserialize(geometry)
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometries"})
    public static byte[] ST_Collect(String[] geometries) throws IOException {
        return GeometrySerde.serialize(
                Functions.createMultiGeometry(
                        GeometrySerde.deserialize(geometries)
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static byte[] ST_CollectionExtract(byte[] geometry) throws IOException {
        return GeometrySerde.serialize(
                Functions.collectionExtract(
                        GeometrySerde.deserialize(geometry)
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry", "geomType"})
    public static byte[] ST_CollectionExtract(byte[] geometry, int geomType) throws IOException {
        return GeometrySerde.serialize(
                Functions.collectionExtract(
                        GeometrySerde.deserialize(geometry),
                        geomType
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry", "pctConvex"})
    public static byte[] ST_ConcaveHull(byte[] geometry, double pctConvex) {
        return GeometrySerde.serialize(
                Functions.concaveHull(
                        GeometrySerde.deserialize(geometry),
                        pctConvex,
                        false
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry", "pctConvex", "allowHoles"})
    public static byte[] ST_ConcaveHull(byte[] geometry, double pctConvex, boolean allowHoles) {
        return GeometrySerde.serialize(
                Functions.concaveHull(
                        GeometrySerde.deserialize(geometry),
                        pctConvex,
                        allowHoles
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"leftGeometry", "rightGeometry"})
    public static boolean ST_Contains(byte[] leftGeometry, byte[] rightGeometry) {
        return Predicates.contains(
                GeometrySerde.deserialize(leftGeometry),
                GeometrySerde.deserialize(rightGeometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static byte[] ST_ConvexHull(byte[] geometry) {
        return GeometrySerde.serialize(
                Functions.convexHull(
                        GeometrySerde.deserialize(geometry)
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"leftGeometry", "rightGeometry"})
    public static boolean ST_CoveredBy(byte[] leftGeometry, byte[] rightGeometry) {
        return Predicates.coveredBy(
                GeometrySerde.deserialize(leftGeometry),
                GeometrySerde.deserialize(rightGeometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"leftGeometry", "rightGeometry"})
    public static boolean ST_Covers(byte[] leftGeometry, byte[] rightGeometry) {
        return Predicates.covers(
                GeometrySerde.deserialize(leftGeometry),
                GeometrySerde.deserialize(rightGeometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"leftGeometry", "rightGeometry"})
    public static boolean ST_Crosses(byte[] leftGeometry, byte[] rightGeometry) {
        return Predicates.crosses(
                GeometrySerde.deserialize(leftGeometry),
                GeometrySerde.deserialize(rightGeometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"leftGeometry", "rightGeometry"})
    public static byte[] ST_Difference(byte[] leftGeometry, byte[] rightGeometry) {
        return GeometrySerde.serialize(
                Functions.difference(
                        GeometrySerde.deserialize(leftGeometry),
                        GeometrySerde.deserialize(rightGeometry)
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"leftGeometry", "rightGeometry"})
    public static boolean ST_Disjoint(byte[] leftGeometry, byte[] rightGeometry) {
        return Predicates.disjoint(
                GeometrySerde.deserialize(leftGeometry),
                GeometrySerde.deserialize(rightGeometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"left", "right"})
    public static double ST_Distance(byte[] left, byte[] right) {
        return Functions.distance(
                GeometrySerde.deserialize(left),
                GeometrySerde.deserialize(right)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"left", "right"})
    public static double ST_3DDistance(byte[] left, byte[] right) {
        return Functions.distance3d(
                GeometrySerde.deserialize(left),
                GeometrySerde.deserialize(right)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static String[] ST_Dump(byte[] geometry) {
        return GeometrySerde.serialize(
                Functions.dump(
                        GeometrySerde.deserialize(geometry)
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static String[] ST_DumpPoints(byte[] geometry) {
        return GeometrySerde.serialize(
                Functions.dumpPoints(
                        GeometrySerde.deserialize(geometry)
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static byte[] ST_EndPoint(byte[] geometry) {
        return GeometrySerde.serialize(
                Functions.endPoint(
                        GeometrySerde.deserialize(geometry)
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static byte[] ST_Envelope(byte[] geometry) {
        return GeometrySerde.serialize(
                Functions.envelope(
                        GeometrySerde.deserialize(geometry)
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"leftGeometry", "rightGeometry"})
    public static boolean ST_Equals(byte[] leftGeometry, byte[] rightGeometry) {
        return Predicates.equals(
                GeometrySerde.deserialize(leftGeometry),
                GeometrySerde.deserialize(rightGeometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static byte[] ST_ExteriorRing(byte[] geometry) {
        return GeometrySerde.serialize(
                Functions.exteriorRing(
                        GeometrySerde.deserialize(geometry)
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static byte[] ST_FlipCoordinates(byte[] geometry) {
        return GeometrySerde.serialize(
                Functions.flipCoordinates(
                        GeometrySerde.deserialize(geometry)
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static byte[] ST_Force_2D(byte[] geometry) {
        return GeometrySerde.serialize(
                Functions.force2D(
                        GeometrySerde.deserialize(geometry)
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry", "precision"})
    public static String ST_GeoHash(byte[] geometry, int precision) {
        return Functions.geohash(
                GeometrySerde.deserialize(geometry),
                precision
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"gml"})
    public static byte[] ST_GeomFromGML(String gml) throws IOException, ParserConfigurationException, SAXException {
        return GeometrySerde.serialize(Constructors.geomFromGML(gml));
    }

    @UDFAnnotations.ParamMeta(argNames = {"geoHash", "precision"})
    public static byte[] ST_GeomFromGeoHash(String geoHash, Integer precision) {
        return GeometrySerde.serialize(
                Constructors.geomFromGeoHash(geoHash, precision)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geoJson"})
    public static byte[] ST_GeomFromGeoJSON(String geoJson) {
        return GeometrySerde.serialize(
                Constructors.geomFromText(geoJson, FileDataSplitter.GEOJSON)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"kml"})
    public static byte[] ST_GeomFromKML(String kml) throws ParseException {
        return GeometrySerde.serialize(
                Constructors.geomFromKML(kml)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"wkt"})
    public static byte[] ST_GeomFromText(String geomString) throws ParseException {
        return GeometrySerde.serialize(
                Constructors.geomFromWKT(geomString, 0)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"wkt", "srid"})
    public static byte[] ST_GeomFromText(String geomString, int srid) throws ParseException {
        return GeometrySerde.serialize(
                Constructors.geomFromWKT(geomString, srid)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"wkb"})
    public static byte[] ST_GeomFromWKB(byte[] wkb) throws ParseException {
        return wkb;
    }

    @UDFAnnotations.ParamMeta(argNames = {"wkt", "srid"})
    public static byte[] ST_GeomFromWKT(String wkt, int srid) throws ParseException {
        return GeometrySerde.serialize(
                Constructors.geomFromWKT(wkt, srid)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"wkt"})
    public static byte[] ST_GeomFromWKT(String wkt) throws ParseException {
        return GeometrySerde.serialize(
                Constructors.geomFromWKT(wkt, 0)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry", "n"})
    public static byte[] ST_GeometryN(byte[] geometry, int n) {
        return GeometrySerde.serialize(
                Functions.geometryN(
                        GeometrySerde.deserialize(geometry),
                        n
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static String ST_GeometryType(byte[] geometry) {
        return Functions.geometryType(
                GeometrySerde.deserialize(geometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry", "n"})
    public static byte[] ST_InteriorRingN(byte[] geometry, int n) {
        return GeometrySerde.serialize(
                Functions.interiorRingN(
                        GeometrySerde.deserialize(geometry),
                        n
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"leftGeometry", "rightGeometry"})
    public static byte[] ST_Intersection(byte[] leftGeometry, byte[] rightGeometry) {
        return GeometrySerde.serialize(
                Functions.intersection(
                        GeometrySerde.deserialize(leftGeometry),
                        GeometrySerde.deserialize(rightGeometry)
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"leftGeometry", "rightGeometry"})
    public static boolean ST_Intersects(byte[] leftGeometry, byte[] rightGeometry) {
        return Predicates.intersects(
                GeometrySerde.deserialize(leftGeometry),
                GeometrySerde.deserialize(rightGeometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static boolean ST_IsClosed(byte[] geometry) {
        return Functions.isClosed(
                GeometrySerde.deserialize(geometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static boolean ST_IsEmpty(byte[] geometry) {
        return Functions.isEmpty(
                GeometrySerde.deserialize(geometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static boolean ST_IsRing(byte[] geometry) {
        return Functions.isRing(
                GeometrySerde.deserialize(geometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static boolean ST_IsSimple(byte[] geometry) {
        return Functions.isSimple(
                GeometrySerde.deserialize(geometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static boolean ST_IsValid(byte[] geometry) {
        return Functions.isValid(
                GeometrySerde.deserialize(geometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static double ST_Length(byte[] geometry) {
        return Functions.length(
                GeometrySerde.deserialize(geometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static byte[] ST_LineFromMultiPoint(byte[] geometry) {
        return GeometrySerde.serialize(
                Functions.lineFromMultiPoint(
                        GeometrySerde.deserialize(geometry)
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geomString"})
    public static byte[] ST_LineFromText(String geomString) {
        return GeometrySerde.serialize(
                Constructors.lineFromText(geomString)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geom", "fraction"})
    public static byte[] ST_LineInterpolatePoint(byte[] geom, double fraction) {
        return GeometrySerde.serialize(
                Functions.lineInterpolatePoint(
                        GeometrySerde.deserialize(geom),
                        fraction
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static byte[] ST_LineMerge(byte[] geometry) {
        return GeometrySerde.serialize(
                Functions.lineMerge(
                        GeometrySerde.deserialize(geometry)
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geomString", "delimiter"})
    public static byte[] ST_LineStringFromText(String geomString, String delimiter) {
        return GeometrySerde.serialize(
                Constructors.lineStringFromText(geomString, delimiter)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geom", "fromFraction", "toFraction"})
    public static byte[] ST_LineSubstring(byte[] geom, double fromFraction, double toFraction) {
        return GeometrySerde.serialize(
                Functions.lineSubString(
                        GeometrySerde.deserialize(geom),
                        fromFraction,
                        toFraction
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"wkt", "srid"})
    public static byte[] ST_MLineFromText(String wkt, int srid) throws ParseException {
        return GeometrySerde.serialize(
                Constructors.mLineFromText(wkt, srid)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"wkt"})
    public static byte[] ST_MLineFromText(String wkt) throws ParseException {
        return GeometrySerde.serialize(
                Constructors.mLineFromText(wkt, 0)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"wkt", "srid"})
    public static byte[] ST_MPolyFromText(String wkt, int srid) throws ParseException {
        return GeometrySerde.serialize(
                Constructors.mPolyFromText(wkt, srid)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"wkt"})
    public static byte[] ST_MPolyFromText(String wkt) throws ParseException {
        return GeometrySerde.serialize(
                Constructors.mPolyFromText(wkt, 0)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"shell"})
    public static byte[] ST_MakePolygon(byte[] shell) {
        return GeometrySerde.serialize(
                Functions.makePolygon(
                        GeometrySerde.deserialize(shell),
                        null
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"shell", "holes"})
    public static byte[] ST_MakePolygon(byte[] shell, String[] holes) {
        return GeometrySerde.serialize(
                Functions.makePolygon(
                        GeometrySerde.deserialize(shell),
                        GeometrySerde.deserialize(holes)
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static byte[] ST_MakeValid(byte[] geometry) {
        return GeometrySerde.serialize(
                Functions.makeValid(
                        GeometrySerde.deserialize(geometry),
                        false
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry", "keepCollapsed"})
    public static byte[] ST_MakeValid(byte[] geometry, boolean keepCollapsed) {
        return GeometrySerde.serialize(
                Functions.makeValid(
                        GeometrySerde.deserialize(geometry),
                        keepCollapsed
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry", "quadrantSegments"})
    public static byte[] ST_MinimumBoundingCircle(byte[] geometry, int quadrantSegments) {
        return GeometrySerde.serialize(
                Functions.minimumBoundingCircle(
                        GeometrySerde.deserialize(geometry),
                        quadrantSegments
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static byte[] ST_Multi(byte[] geometry) throws IOException {
        return GeometrySerde.serialize(
                Functions.createMultiGeometryFromOneElement(
                        GeometrySerde.deserialize(geometry)
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static int ST_NDims(byte[] geometry) {
        return Functions.nDims(
                GeometrySerde.deserialize(geometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static int ST_NPoints(byte[] geometry) {
        return Functions.nPoints(
                GeometrySerde.deserialize(geometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static byte[] ST_Normalize(byte[] geometry) {
        return GeometrySerde.serialize(
                Functions.normalize(
                        GeometrySerde.deserialize(geometry)
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static int ST_NumGeometries(byte[] geometry) {
        return Functions.numGeometries(
                GeometrySerde.deserialize(geometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static Integer ST_NumInteriorRings(byte[] geometry) {
        return Functions.numInteriorRings(
                GeometrySerde.deserialize(geometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"leftGeometry", "rightGeometry"})
    public static boolean ST_OrderingEquals(byte[] leftGeometry, byte[] rightGeometry) {
        return Predicates.orderingEquals(
                GeometrySerde.deserialize(leftGeometry),
                GeometrySerde.deserialize(rightGeometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"leftGeometry", "rightGeometry"})
    public static boolean ST_Overlaps(byte[] leftGeometry, byte[] rightGeometry) {
        return Predicates.overlaps(
                GeometrySerde.deserialize(leftGeometry),
                GeometrySerde.deserialize(rightGeometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"x", "y"})
    public static byte[] ST_Point(double x, double y) {
        return GeometrySerde.serialize(
                Constructors.point(x, y)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geomString", "geomFormat"})
    public static byte[] ST_PointFromText(String geomString, String geomFormat) {
        return GeometrySerde.serialize(
                Constructors.pointFromText(geomString, geomFormat)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry", "n"})
    public static byte[] ST_PointN(byte[] geometry, int n) {
        return GeometrySerde.serialize(
                Functions.pointN(
                        GeometrySerde.deserialize(geometry),
                        n
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static byte[] ST_PointOnSurface(byte[] geometry) {
        return GeometrySerde.serialize(
                Functions.pointOnSurface(
                        GeometrySerde.deserialize(geometry)
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"x", "y", "z"})
    public static byte[] ST_PointZ(double x, double y, double z) {
        return GeometrySerde.serialize(
                Constructors.pointZ(x, y, z, 0)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"x", "y", "z", "srid"})
    public static byte[] ST_PointZ(double x, double y, double z, int srid) {
        return GeometrySerde.serialize(
                Constructors.pointZ(x, y, z, srid)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"minX", "minY", "maxX", "maxY"})
    public static byte[] ST_PolygonFromEnvelope(double minX, double minY, double maxX, double maxY) {
        return GeometrySerde.serialize(
                Constructors.polygonFromEnvelope(minX, minY, maxX, maxY)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geomString", "delimiter"})
    public static byte[] ST_PolygonFromText(String geomString, String geomFormat) {
        return GeometrySerde.serialize(
                Constructors.polygonFromText(geomString, geomFormat)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry", "precisionScale"})
    public static byte[] ST_PrecisionReduce(byte[] geometry, int precisionScale) {
        return GeometrySerde.serialize(
                Functions.reducePrecision(
                        GeometrySerde.deserialize(geometry),
                        precisionScale
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"linestring"})
    public static byte[] ST_RemovePoint(byte[] linestring) {
        return GeometrySerde.serialize(
                Functions.removePoint(
                        GeometrySerde.deserialize(linestring)
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"linestring", "position"})
    public static byte[] ST_RemovePoint(byte[] linestring, int position) {
        return GeometrySerde.serialize(
                Functions.removePoint(
                        GeometrySerde.deserialize(linestring),
                        position
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static byte[] ST_Reverse(byte[] geometry) {
        return GeometrySerde.serialize(
                Functions.reverse(
                        GeometrySerde.deserialize(geometry)
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"input", "level"})
    public static long[] ST_S2CellIDs(byte[] input, int level) {
        return TypeUtils.castLong(
                Functions.s2CellIDs(
                        GeometrySerde.deserialize(input),
                        level
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static int ST_SRID(byte[] geometry) {
        return Functions.getSRID(
                GeometrySerde.deserialize(geometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static String ST_AsText(byte[] geometry) {
        return Functions.asWKT(
                GeometrySerde.deserialize(geometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"linestring", "position", "point"})
    public static byte[] ST_SetPoint(byte[] linestring, int position, byte[] point) {
        return GeometrySerde.serialize(
                Functions.setPoint(
                        GeometrySerde.deserialize(linestring),
                        position,
                        GeometrySerde.deserialize(point)
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry", "srid"})
    public static byte[] ST_SetSRID(byte[] geometry, int srid) {
        return GeometrySerde.serialize(
                Functions.setSRID(
                        GeometrySerde.deserialize(geometry),
                        srid
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry", "distanceTolerance"})
    public static byte[] ST_SimplifyPreserveTopology(byte[] geometry, double distanceTolerance) {
        return GeometrySerde.serialize(
                Functions.simplifyPreserveTopology(
                        GeometrySerde.deserialize(geometry),
                        distanceTolerance
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"input", "blade"})
    public static byte[] ST_Split(byte[] input, byte[] blade) {
        return GeometrySerde.serialize(
                Functions.split(
                        GeometrySerde.deserialize(input),
                        GeometrySerde.deserialize(blade)
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static byte[] ST_StartPoint(byte[] geometry) {
        return GeometrySerde.serialize(
                Functions.startPoint(
                        GeometrySerde.deserialize(geometry)
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry", "maxVertices"})
    public static String[] ST_SubDivide(byte[] geometry, int maxVertices) {
        return GeometrySerde.serialize(
                Functions.subDivide(
                        GeometrySerde.deserialize(geometry),
                        maxVertices
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"leftGeom", "rightGeom"})
    public static byte[] ST_SymDifference(byte[] leftGeom, byte[] rightGeom) {
        return GeometrySerde.serialize(
                Functions.symDifference(
                        GeometrySerde.deserialize(leftGeom),
                        GeometrySerde.deserialize(rightGeom)
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"leftGeometry", "rightGeometry"})
    public static boolean ST_Touches(byte[] leftGeometry, byte[] rightGeometry) {
        return Predicates.touches(
                GeometrySerde.deserialize(leftGeometry),
                GeometrySerde.deserialize(rightGeometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry", "sourceCRS", "targetCRS"})
    public static byte[] ST_Transform(byte[] geometry, String sourceCRS, String targetCRS) {
        return GeometrySerde.serialize(
                GeoToolsWrapper.transform(
                        GeometrySerde.deserialize(geometry),
                        sourceCRS,
                        targetCRS
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry", "sourceCRS", "targetCRS", "lenient"})
    public static byte[] ST_Transform(byte[] geometry, String sourceCRS, String targetCRS, boolean lenient) {
        return GeometrySerde.serialize(
                GeoToolsWrapper.transform(
                        GeometrySerde.deserialize(geometry),
                        sourceCRS,
                        targetCRS,
                        lenient
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"leftGeom", "rightGeom"})
    public static byte[] ST_Union(byte[] leftGeom, byte[] rightGeom) {
        return GeometrySerde.serialize(
                Functions.union(
                        GeometrySerde.deserialize(leftGeom),
                        GeometrySerde.deserialize(rightGeom)
                )
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"leftGeometry", "rightGeometry"})
    public static boolean ST_Within(byte[] leftGeometry, byte[] rightGeometry) {
        return Predicates.within(
                GeometrySerde.deserialize(leftGeometry),
                GeometrySerde.deserialize(rightGeometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static Double ST_X(byte[] geometry) {
        return Functions.x(
                GeometrySerde.deserialize(geometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static double ST_XMax(byte[] geometry) {
        return Functions.xMax(
                GeometrySerde.deserialize(geometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static double ST_XMin(byte[] geometry) {
        return Functions.xMin(
                GeometrySerde.deserialize(geometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static Double ST_Y(byte[] geometry) {
        return Functions.y(
                GeometrySerde.deserialize(geometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static double ST_YMax(byte[] geometry) {
        return Functions.yMax(
                GeometrySerde.deserialize(geometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static double ST_YMin(byte[] geometry) {
        return Functions.yMin(
                GeometrySerde.deserialize(geometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static Double ST_Z(byte[] geometry) {
        return Functions.z(
                GeometrySerde.deserialize(geometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static Double ST_ZMax(byte[] geometry) {
        return Functions.zMax(
                GeometrySerde.deserialize(geometry)
        );
    }

    @UDFAnnotations.ParamMeta(argNames = {"geometry"})
    public static Double ST_ZMin(byte[] geometry) {
        return Functions.zMin(
                GeometrySerde.deserialize(geometry)
        );
    }
}