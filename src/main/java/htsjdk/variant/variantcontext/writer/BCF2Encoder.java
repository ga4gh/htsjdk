/*
* Copyright (c) 2012 The Broad Institute
* 
* Permission is hereby granted, free of charge, to any person
* obtaining a copy of this software and associated documentation
* files (the "Software"), to deal in the Software without
* restriction, including without limitation the rights to use,
* copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the
* Software is furnished to do so, subject to the following
* conditions:
* 
* The above copyright notice and this permission notice shall be
* included in all copies or substantial portions of the Software.
* 
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
* EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
* OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
* NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
* HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
* WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
* FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR
* THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

package htsjdk.variant.variantcontext.writer;

import htsjdk.tribble.TribbleException;
import htsjdk.variant.bcf2.BCF2Type;
import htsjdk.variant.bcf2.BCF2Utils;
import htsjdk.variant.bcf2.BCFVersion;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * See #BCFWriter for documentation on this classes role in encoding BCF2 files
 *
 * @author Mark DePristo
 * @since 06/12
 */
public abstract class BCF2Encoder {
    // TODO -- increase default size?
    public static final int WRITE_BUFFER_INITIAL_SIZE = 16384;
    protected final ByteArrayOutputStream encodeStream = new ByteArrayOutputStream(WRITE_BUFFER_INITIAL_SIZE);

    public static BCF2Encoder getEncoder(final BCFVersion version) {
        switch (version.getMinorVersion()) {
            case 1:
                return new BCF2_1Encoder();
            case 2:
                return new BCF2_2Encoder();
            default:
                throw new TribbleException("BCF2Codec can only process BCF2 files with minor version <= " + 2 + " but this file has minor version " + version.getMinorVersion());
        }
    }

    // --------------------------------------------------------------------------------
    //
    // Functions to return the data being encoded here
    //
    // --------------------------------------------------------------------------------

    public byte[] getRecordBytes() {
        final byte[] bytes = encodeStream.toByteArray();
        encodeStream.reset();
        return bytes;
    }

    // --------------------------------------------------------------------------------
    //
    // Writing typed values (have type byte)
    //
    // --------------------------------------------------------------------------------

    public final void encodeTypedMissing(final BCF2Type type) throws IOException {
        encodeType(0, type);
    }

    public final void encodeTyped(final Object value, final BCF2Type type) throws IOException {
        if (value == null)
            encodeTypedMissing(type);
        else {
            switch (type) {
                case INT8:
                case INT16:
                case INT32:
                    encodeTypedInt((Integer) value, type);
                    break;
                case FLOAT:
                    encodeTypedFloat((Double) value);
                    break;
                case CHAR:
                    encodeTypedString((String) value);
                    break;
                default:
                    throw new IllegalArgumentException("Illegal type encountered " + type);
            }
        }
    }

    public final void encodeTypedInt(final int v) throws IOException {
        final BCF2Type type = BCF2Utils.determineIntegerType(v);
        encodeTypedInt(v, type);
    }

    public final void encodeTypedInt(final int v, final BCF2Type type) throws IOException {
        encodeType(1, type);
        encodeRawInt(v, type);
    }

    public final void encodeTypedString(final String s) throws IOException {
        encodeTypedString(s.getBytes());
    }

    public final void encodeTypedString(final byte[] s, final int padding) throws IOException {
        encodeType(s.length, BCF2Type.CHAR);
        for (final byte b : s) {
            encodeRawChar(b);
        }
        encodePaddingValues(padding - s.length, BCF2Type.CHAR);
    }

    public final void encodeTypedString(final byte[] s) throws IOException {
        encodeType(s.length, BCF2Type.CHAR);
        for (final byte b : s) {
            encodeRawChar(b);
        }
    }

    public final void encodeTypedFloat(final double d) throws IOException {
        encodeType(1, BCF2Type.FLOAT);
        encodeRawFloat(d);
    }

    public void encodeTypedVecInt(final int[] vs) throws IOException {
        final int size = vs.length;
        final BCF2Type type = BCF2Utils.determineIntegerType(vs);
        encodeType(size, type);
        encodeRawVecInt(vs, size, type);
    }

    public void encodeTypedVecInt(final int[] vs, final int size) throws IOException {
        final BCF2Type type = BCF2Utils.determineIntegerType(vs);
        encodeType(size, type);
        encodeRawVecInt(vs, size, type);
    }

    public void encodeTypedVecInt(final List<Integer> vs) throws IOException {
        final int size = vs.size();
        final BCF2Type type = BCF2Utils.determineIntegerType(vs);
        encodeType(size, type);
        encodeRawVecInt(vs, size, type);
    }

    public void encodeTypedVecInt(final List<Integer> vs, final int size) throws IOException {
        final BCF2Type type = BCF2Utils.determineIntegerType(vs);
        encodeType(size, type);
        encodeRawVecInt(vs, size, type);
    }

    public void encodeRawVecInt(final int[] vs, final int padding, final BCF2Type type) throws IOException {
        for (final int v : vs) {
            type.write(v, encodeStream);
        }
        encodePaddingValues(padding - vs.length, type);

    }

    public void encodeRawVecInt(final List<Integer> vs, final int padding, final BCF2Type type) throws IOException {
        for (final Integer v : vs) {
            if (v == null) {
                type.write(type.getMissingBytes(), encodeStream);
            } else {
                type.write(v, encodeStream);
            }
        }
        encodePaddingValues(padding - vs.size(), type);
    }

    public void encodeTypedVecFLoat(final double[] vs, final int padding) throws IOException {
        encodeType(padding, BCF2Type.FLOAT);
        encodeRawVecFLoat(vs, padding);
    }

    public void encodeTypedVecFLoat(final List<Double> vs, final int padding) throws IOException {
        encodeType(padding, BCF2Type.FLOAT);
        encodeRawVecFLoat(vs, padding);
    }

    public void encodeRawVecFLoat(final double[] vs, final int padding) throws IOException {
        for (final double v : vs) {
            encodeRawFloat(v);
        }
        encodePaddingValues(padding - vs.length, BCF2Type.FLOAT);
    }

    public void encodeRawVecFLoat(final List<Double> vs, final int padding) throws IOException {
        for (final Double v : vs) {
            if (v == null) {
                encodeRawMissingValue(BCF2Type.FLOAT);
            } else {
                encodeRawFloat(v);
            }
        }
        encodePaddingValues(padding - vs.size(), BCF2Type.FLOAT);
    }

    public void encodePaddingValues(final int size, final BCF2Type type) throws IOException {
        for (int i = 0; i < size; i++) {
            encodePaddingValue(type);
        }
    }

    public abstract void encodePaddingValue(final BCF2Type type) throws IOException;

    public final void encodeTyped(List<?> v, final BCF2Type type) throws IOException {
        if (type == BCF2Type.CHAR && !v.isEmpty()) {
            final String s = BCF2Utils.collapseStringList((List<String>) v);
            v = stringToBytes(s);
        }

        encodeType(v.size(), type);
        encodeRawValues(v, type);
    }

    // --------------------------------------------------------------------------------
    //
    // Writing raw values (don't have a type byte)
    //
    // --------------------------------------------------------------------------------

    public final <T> void encodeRawValues(final Collection<T> v, final BCF2Type type) throws IOException {
        for (final T v1 : v) {
            encodeRawValue(v1, type);
        }
    }

    public final <T> void encodeRawValue(final T value, final BCF2Type type) throws IOException {
        try {
            if (value == type.getMissingJavaValue())
                encodeRawMissingValue(type);
            else {
                switch (type) {
                    case INT8:
                    case INT16:
                    case INT32:
                        encodeRawBytes((Integer) value, type);
                        break;
                    case FLOAT:
                        encodeRawFloat((Double) value);
                        break;
                    case CHAR:
                        encodeRawChar((Byte) value);
                        break;
                    default:
                        throw new IllegalArgumentException("Illegal type encountered " + type);
                }
            }
        } catch (final ClassCastException e) {
            throw new ClassCastException("BUG: invalid type cast to " + type + " from " + value);
        }
    }

    public final void encodeRawMissingValue(final BCF2Type type) throws IOException {
        encodeRawBytes(type.getMissingBytes(), type);
    }

    public final void encodeRawMissingValues(final int size, final BCF2Type type) throws IOException {
        for (int i = 0; i < size; i++)
            encodeRawMissingValue(type);
    }

    // --------------------------------------------------------------------------------
    //
    // low-level encoders
    //
    // --------------------------------------------------------------------------------

    public final void encodeRawChar(final byte c) {
        encodeStream.write(c);
    }

    public final void encodeRawFloat(final double value) throws IOException {
        encodeRawBytes(Float.floatToIntBits((float) value), BCF2Type.FLOAT);
    }

    public final void encodeType(final int size, final BCF2Type type) throws IOException {
        if (size <= BCF2Utils.MAX_INLINE_ELEMENTS) {
            final int typeByte = BCF2Utils.encodeTypeDescriptor(size, type);
            encodeStream.write(typeByte);
        } else {
            final int typeByte = BCF2Utils.encodeTypeDescriptor(BCF2Utils.OVERFLOW_ELEMENT_MARKER, type);
            encodeStream.write(typeByte);
            // write in the overflow size
            encodeTypedInt(size);
        }
    }

    public final void encodeRawInt(final int value, final BCF2Type type) throws IOException {
        type.write(value, encodeStream);
    }

    public final void encodeRawBytes(final int value, final BCF2Type type) throws IOException {
        type.write(value, encodeStream);
    }

    // --------------------------------------------------------------------------------
    //
    // utility functions
    //
    // --------------------------------------------------------------------------------

    public void encodeRawString(final byte[] bytes, final int sizeToWrite) throws IOException {
        for (final byte b : bytes)
            BCF2Type.CHAR.write(b, encodeStream);
        for (int i = sizeToWrite - bytes.length; i > 0; i--)
            // Pad with zeros, see https://github.com/samtools/hts-specs/issues/232
            BCF2Type.CHAR.write(0, encodeStream);
    }

    public byte[] compactStrings(final String[] strings) {
        return compactStrings(Arrays.asList(strings));
    }

    public abstract byte[] compactStrings(final List<String> strings);

    /**
     * Totally generic encoder that examines o, determines the best way to encode it, and encodes it
     * <p>
     * This method is incredibly slow, but it's only used for UnitTests so it doesn't matter
     *
     * @param o
     * @return
     */
    public final BCF2Type encode(final Object o) throws IOException {
        if (o == null) throw new IllegalArgumentException("Generic encode cannot deal with null values");

        if (o instanceof List) {
            final BCF2Type type = determineBCFType(((List) o).get(0));
            encodeTyped((List) o, type);
            return type;
        } else {
            final BCF2Type type = determineBCFType(o);
            encodeTyped(o, type);
            return type;
        }
    }

    private BCF2Type determineBCFType(final Object arg) {
        final Object toType = arg instanceof List ? ((List) arg).get(0) : arg;

        if (toType instanceof Integer)
            return BCF2Utils.determineIntegerType((Integer) toType);
        else if (toType instanceof String)
            return BCF2Type.CHAR;
        else if (toType instanceof Double)
            return BCF2Type.FLOAT;
        else
            throw new IllegalArgumentException("No native encoding for Object of type " + arg.getClass().getSimpleName());
    }

    private List<Byte> stringToBytes(final String v) {
        if (v == null || v.equals(""))
            return Collections.emptyList();
        else {
            // TODO -- this needs to be optimized away for efficiency
            final byte[] bytes = v.getBytes();
            final List<Byte> l = new ArrayList<>(bytes.length);
            for (final byte aByte : bytes) l.add(aByte);
            return l;
        }
    }

    public static class BCF2_1Encoder extends BCF2Encoder {

        @Override
        public void encodePaddingValue(final BCF2Type type) throws IOException {
            type.write(type.getMissingBytes(), encodeStream);
        }

        @Override
        public byte[] compactStrings(final List<String> strings) {
            // Sum of lengths of individual strings, plus 1 comma for each string except the first
            int size = strings.size();
            final byte[][] bytes = new byte[strings.size()][];
            int i = 0;
            for (final String s : strings) {
                bytes[i] = s.getBytes(StandardCharsets.UTF_8);
                size += bytes[i].length;
                i++;
            }
            final ByteBuffer buff = ByteBuffer.allocate(size);
            for (final byte[] bs : bytes) {
                buff.put((byte) ',');
                buff.put(bs);
            }

            return buff.array();
        }
    }

    public static class BCF2_2Encoder extends BCF2Encoder {

        @Override
        public void encodePaddingValue(final BCF2Type type) throws IOException {
            type.write(type.getEOVBytes(), encodeStream);
        }

        @Override
        public byte[] compactStrings(final List<String> strings) {
            // Sum of lengths of individual strings, plus 1 comma for each string except the first
            int size = strings.size() - 1;
            final byte[][] bytes = new byte[strings.size()][];
            int i = 0;
            for (final String s : strings) {
                bytes[i] = s.getBytes(StandardCharsets.UTF_8);
                size += bytes[i].length;
                i++;
            }
            final ByteBuffer buff = ByteBuffer.allocate(size);
            boolean first = true;
            for (final byte[] bs : bytes) {
                if (first) first = false;
                else buff.put((byte) ',');
                buff.put(bs);
            }

            return buff.array();
        }
    }
}