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
package org.apache.sedona.common.raster;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.sedona.common.raster.outdb.OutDbGridCoverage2D;
import org.geotools.coverage.Category;
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.opengis.referencing.operation.MathTransform;

import javax.media.jai.RenderedImageAdapter;
import java.awt.image.RenderedImage;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.List;

public class Serde {

    static final Field field;
    static {
        try {
            field = GridCoverage2D.class.getDeclaredField("serializedImage");
            field.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * GridSampleDimension and RenderedSampleDimension are not serializable. We need to provide a custom serializer
     */
    private static class KryoGridSampleDimensionSerializer extends Serializer<GridSampleDimension> {
        @Override
        public void write(Kryo kryo, Output output, GridSampleDimension sampleDimension) {
            String description = sampleDimension.getDescription().toString();
            List<Category> categories = sampleDimension.getCategories();
            double offset = sampleDimension.getOffset();
            double scale = sampleDimension.getScale();
            output.writeString(description);
            kryo.writeObject(output, categories.toArray());
            output.writeDouble(offset);
            output.writeDouble(scale);
        }

        @Override
        public GridSampleDimension read(Kryo kryo, Input input, Class aClass) {
            String description = input.readString();
            Category[] categories = kryo.readObject(input, Category[].class);
            double offset = input.readDouble();
            double scale = input.readDouble();
            return new GridSampleDimension(description, categories, offset, scale);
        }
    }

    /**
     * AffineTransform2D cannot be correctly deserialized by the default serializer of Kryo, so we need to provide a
     * custom serializer.
     */
    private static class KryoAffineTransform2DSerializer extends Serializer<AffineTransform2D> {
        @Override
        public void write(Kryo kryo, Output output, AffineTransform2D affineTransform2D) {
            output.writeDouble(affineTransform2D.getScaleX());
            output.writeDouble(affineTransform2D.getShearY());
            output.writeDouble(affineTransform2D.getShearX());
            output.writeDouble(affineTransform2D.getScaleY());
            output.writeDouble(affineTransform2D.getTranslateX());
            output.writeDouble(affineTransform2D.getTranslateY());
        }

        @Override
        public AffineTransform2D read(Kryo kryo, Input input, Class<AffineTransform2D> aClass) {
            double scaleX = input.readDouble();
            double skewY = input.readDouble();
            double skewX = input.readDouble();
            double scaleY = input.readDouble();
            double upperLeftX = input.readDouble();
            double upperLeftY = input.readDouble();
            return new AffineTransform2D(scaleX, skewY, skewX, scaleY, upperLeftX, upperLeftY);
        }
    }

    /**
     * URIs are not serializable. We need to provide a custom serializer
     */
    private static class URISerializer extends Serializer<java.net.URI> {
        public URISerializer() {
            setImmutable(true);
        }

        @Override
        public void write(final Kryo kryo, final Output output, final URI uri) {
            output.writeString(uri.toString());
        }

        @Override
        public URI read(final Kryo kryo, final Input input, final Class<URI> uriClass) {
            return URI.create(input.readString());
        }
    }

    private static final ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
            kryo.register(AffineTransform2D.class, new KryoAffineTransform2DSerializer());
            kryo.register(GridSampleDimension.class, new KryoGridSampleDimensionSerializer());
            kryo.register(URI.class, new URISerializer());
            // DeepCopiedRenderedImage has a well written serializer, so we use the default one.
            kryo.register(DeepCopiedRenderedImage.class, new JavaSerializer());
            try {
                kryo.register(Class.forName("org.geotools.coverage.grid.RenderedSampleDimension"),
                        new KryoGridSampleDimensionSerializer());
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Cannot register kryo serializer for class RenderedSampleDimension", e);
            }
            kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
            return kryo;
        }
    };

    private static class SerializableState implements Serializable {
        public CharSequence name;

        // The following three components are used to construct a GridGeometry2D object.
        // We serialize CRS separately because the default serializer is pretty slow, we use a
        // cached serializer to speed up the serialization and reuse CRS on deserialization.
        public GridEnvelope2D gridEnvelope2D;
        public MathTransform gridToCRS;
        public byte[] serializedCRS;

        public GridSampleDimension[] bands;
        public DeepCopiedRenderedImage image;

        public GridCoverage2D restore() {
            GridGeometry2D gridGeometry = new GridGeometry2D(gridEnvelope2D, gridToCRS, CRSSerializer.deserialize(serializedCRS));
            return new GridCoverageFactory().create(name, image, gridGeometry, bands, null, null);
        }
    }

    public static byte[] serialize(GridCoverage2D raster) throws IOException {
        return serialize(raster, true);
    }

    public static byte[] serialize(GridCoverage2D raster, boolean withOutDbConfiguration) throws IOException {
        Kryo kryo = kryos.get();
        if (!(raster instanceof OutDbGridCoverage2D)) {
            // GridCoverage2D created by GridCoverage2DReaders contain references that are not serializable.
            // Wrap the RenderedImage in DeepCopiedRenderedImage to make it serializable.
            DeepCopiedRenderedImage deepCopiedRenderedImage = null;
            RenderedImage renderedImage = raster.getRenderedImage();
            while (renderedImage instanceof RenderedImageAdapter) {
                renderedImage = ((RenderedImageAdapter) renderedImage).getWrappedImage();
            }
            if (renderedImage instanceof DeepCopiedRenderedImage) {
                deepCopiedRenderedImage = (DeepCopiedRenderedImage) renderedImage;
            } else {
                deepCopiedRenderedImage = new DeepCopiedRenderedImage(renderedImage);
            }

            SerializableState state = new SerializableState();
            GridGeometry2D gridGeometry = raster.getGridGeometry();
            state.name = raster.getName();
            state.gridEnvelope2D = gridGeometry.getGridRange2D();
            state.gridToCRS = gridGeometry.getGridToCRS2D();
            state.serializedCRS = CRSSerializer.serialize(gridGeometry.getCoordinateReferenceSystem());
            state.bands = raster.getSampleDimensions();
            state.image = deepCopiedRenderedImage;
            try (Output out = new Output(4096, -1)) {
                out.writeBoolean(false);
                kryo.writeObject(out, state);
                return out.toBytes();
            }
        } else {
            // Get a serializable state of OutDbGridCoverage2D and serialize it. We can restore the OutDbGridCoverage2D
            // object from that state on deserialization.
            OutDbGridCoverage2D outDbRaster = (OutDbGridCoverage2D) raster;
            OutDbGridCoverage2D.SerializableState state = outDbRaster.getSerializableState(withOutDbConfiguration);
            try (Output out = new Output(4096, -1)) {
                out.writeBoolean(true);
                kryo.writeObject(out, state);
                return out.toBytes();
            }
        }
    }

    public static GridCoverage2D deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        return deserialize(bytes, null);
    }

    public static GridCoverage2D deserialize(byte[] bytes, byte[] serializedConf) throws IOException, ClassNotFoundException {
        Kryo kryo = kryos.get();
        try (Input in = new Input(bytes)) {
            boolean isOutDb = in.readBoolean();
            if (!isOutDb) {
                SerializableState state = kryo.readObject(in, SerializableState.class);
                return state.restore();
            } else {
                OutDbGridCoverage2D.SerializableState state = kryo.readObject(in, OutDbGridCoverage2D.SerializableState.class);
                if (serializedConf != null) {
                    return state.restore(serializedConf);
                } else {
                    return state.restore();
                }
            }
        }
    }
}
