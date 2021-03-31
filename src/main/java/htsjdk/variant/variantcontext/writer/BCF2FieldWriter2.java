package htsjdk.variant.variantcontext.writer;

import htsjdk.tribble.TribbleException;
import htsjdk.variant.bcf2.BCF2Type;
import htsjdk.variant.bcf2.BCF2Utils;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCompoundHeaderLine;
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import htsjdk.variant.vcf.VCFHeaderLineCount;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Encoders for both INFO and FORMAT fields.
 * <p>
 * Although BCF formally treats atomic values as special cases of vectors with 1 element,
 * atomic writers are separated from vector writers here for efficiency where applicable.
 * <p>
 * The writers are designed to accommodate the weak typing of the VariantContext class's
 * attributes field while avoiding unnecessary allocations to force the data into a uniform
 * shape, and to avoid unnecessary passes over the data during writing.
 * <p>
 * Specialized writers are present to encode attributes of Genotype objects that are stored
 * inline in the object and not in its attributes map.
 */
public abstract class BCF2FieldWriter2 {

    protected final VCFCompoundHeaderLine headerLine;

    protected final BCF2Encoder encoder;

    protected final int dictionaryOffset;

    protected final BCF2Type dictionaryOffsetType;

    protected final String key;

    protected final boolean unbounded;

    public BCF2FieldWriter2(final VCFCompoundHeaderLine headerLine, final BCF2Encoder encoder, final int dictionaryOffset) {
        this.headerLine = headerLine;
        this.encoder = encoder;
        this.dictionaryOffset = dictionaryOffset;
        this.dictionaryOffsetType = BCF2Utils.determineIntegerType(dictionaryOffset);
        this.key = headerLine.getID();
        unbounded = headerLine.getCountType() == VCFHeaderLineCount.UNBOUNDED;
    }

    /**
     * Base class for writers that encode INFO or site data.
     * <p>
     * This process is simpler than encoding genotype data, because INFO data
     * only consists of a single typed value whose size and type can be determined by
     * inspecting the value, and no padding is required.
     */
    public static abstract class InfoWriter extends BCF2FieldWriter2 {

        public InfoWriter(final VCFInfoHeaderLine headerLine, final BCF2Encoder encoder, final int dictionaryOffset) {
            super(headerLine, encoder, dictionaryOffset);
        }

        public void encode(final VariantContext vc) throws IOException {
            encoder.encodeTypedInt(dictionaryOffset, dictionaryOffsetType);
            write(vc);
        }

        protected abstract void write(final VariantContext vc) throws IOException;
    }

    public static class AtomicIntInfoWriter extends InfoWriter {

        public AtomicIntInfoWriter(final VCFInfoHeaderLine headerLine, final BCF2Encoder encoder, final int dictionaryOffset) {
            super(headerLine, encoder, dictionaryOffset);
        }

        @Override
        public void write(final VariantContext vc) throws IOException {
            final Object o = vc.getAttribute(key);
            if (o == null) {
                encoder.encodeTypedMissing(BCF2Type.INT8);
            } else if (o instanceof Integer) {
                final int v = (Integer) o;
                encoder.encodeTypedInt(v);
            } else {
                throw new TribbleException("Found attribute of incompatible type: " + o.getClass().getSimpleName());
            }
        }
    }

    public static class AtomicFloatInfoWriter extends InfoWriter {

        public AtomicFloatInfoWriter(final VCFInfoHeaderLine headerLine, final BCF2Encoder encoder, final int dictionaryOffset) {
            super(headerLine, encoder, dictionaryOffset);
        }

        @Override
        public void write(final VariantContext vc) throws IOException {
            final Object o = vc.getAttribute(key);
            if (o == null) {
                encoder.encodeTypedMissing(BCF2Type.FLOAT);
            } else if (o instanceof Double) {
                final double d = (Double) o;
                encoder.encodeTypedFloat(d);
            } else {
                throw new TribbleException("Found attribute of incompatible type: " + o.getClass().getSimpleName());
            }
        }
    }

    public static class FlagInfoWriter extends InfoWriter {

        public FlagInfoWriter(final VCFInfoHeaderLine headerLine, final BCF2Encoder encoder, final int dictionaryOffset) {
            super(headerLine, encoder, dictionaryOffset);
        }

        @Override
        public void write(final VariantContext vc) throws IOException {
            final Object o;
            if ((o = vc.getAttribute(key)) != null && o instanceof Boolean) {
                encoder.encodeTypedInt(1, BCF2Type.INT8);
            } else {
                throw new TribbleException("VariantContext did not find attribute of type FLAG associated with key: " + key);
            }
        }
    }

    public static class VecIntInfoWriter extends InfoWriter {

        private int nValues;

        public VecIntInfoWriter(final VCFInfoHeaderLine headerLine, final BCF2Encoder encoder, final int dictionaryOffset) {
            super(headerLine, encoder, dictionaryOffset);
        }

        @Override
        public void write(final VariantContext vc) throws IOException {
            final Object o = vc.getAttribute(key);
            if (o == null) {
                encoder.encodeTypedMissing(BCF2Type.INT8);
            } else if (o instanceof List) {
                final List<Integer> vs = (List<Integer>) o;
                validateSize(vs.size(), vc);
                encoder.encodeTypedVecInt(vs, nValues);
            } else if (o instanceof Integer) {
                // Arrays that were pruned down to 1 element may be stored as the bare type instead of a List
                final int v = (Integer) o;
                final BCF2Type type = BCF2Utils.determineIntegerType(v);
                validateSize(1, vc);
                encoder.encodeType(nValues, type);
                encoder.encodeRawFloat(v);
                encoder.encodePaddingValues(nValues - 1, type);
            } else if (o.getClass().isArray()) {
                final int[] vs = (int[]) o;
                validateSize(vs.length, vc);
                encoder.encodeTypedVecInt(vs, nValues);
            } else {
                throw new TribbleException("Found attribute of incompatible type: " + o.getClass().getSimpleName());
            }
        }

        private void validateSize(final int observedValues, final VariantContext vc) {
            if (unbounded) {
                nValues = observedValues;
            } else {
                nValues = headerLine.getCount(vc);
                if (observedValues > nValues)
                    throw new TribbleException("Observed number of values: " + observedValues + " exceeds expected number: " + nValues);
            }
        }
    }

    public static class VecFloatInfoWriter extends InfoWriter {

        private static final BCF2Type type = BCF2Type.FLOAT;

        private int nValues;

        public VecFloatInfoWriter(final VCFInfoHeaderLine headerLine, final BCF2Encoder encoder, final int dictionaryOffset) {
            super(headerLine, encoder, dictionaryOffset);
        }

        @Override
        public void write(final VariantContext vc) throws IOException {
            final Object o = vc.getAttribute(key);
            if (o == null) {
                encoder.encodeTypedMissing(type);
            } else if (o instanceof List) {
                final List<Double> vs = (List<Double>) o;
                validateSize(vs.size(), vc);
                encoder.encodeTypedVecFLoat(vs, nValues);
            } else if (o instanceof Double) {
                // Arrays that were pruned down to 1 element may be stored as the bare type instead of a List
                final double v = (Double) o;
                validateSize(1, vc);
                encoder.encodeType(nValues, type);
                encoder.encodeRawFloat(v);
                encoder.encodePaddingValues(nValues - 1, type);
            } else if (o.getClass().isArray()) {
                final double[] vs = (double[]) o;
                validateSize(vs.length, vc);
                encoder.encodeTypedVecFLoat(vs, nValues);
            } else {
                throw new TribbleException("Found attribute of incompatible type: " + o.getClass().getSimpleName());
            }
        }

        private void validateSize(final int observedValues, final VariantContext vc) {
            if (unbounded) {
                nValues = observedValues;
            } else {
                nValues = headerLine.getCount(vc);
                if (observedValues > nValues)
                    throw new TribbleException("Observed number of values: " + observedValues + " exceeds expected number: " + nValues);
            }
        }
    }

    public static class CharInfoWriter extends InfoWriter {

        public CharInfoWriter(final VCFInfoHeaderLine headerLine, final BCF2Encoder encoder, final int dictionaryOffset) {
            super(headerLine, encoder, dictionaryOffset);
        }

        @Override
        public void write(final VariantContext vc) throws IOException {
            final Object o = vc.getAttribute(key);
            if (o == null) {
                encoder.encodeTypedMissing(BCF2Type.CHAR);
            } else if (o instanceof String) {
                final byte[] bytes = ((String) o).getBytes(StandardCharsets.UTF_8);
                final int observedValues = bytes.length;
                final int nValues;
                if (unbounded) {
                    nValues = observedValues;
                } else {
                    nValues = headerLine.getCount(vc);
                    if (observedValues > nValues)
                        throw new TribbleException("Observed number of values: " + observedValues + " exceeds expected number: " + nValues);
                }
                encoder.encodeTypedString(bytes, nValues);
            } else {
                throw new TribbleException("Found attribute of incompatible type: " + o.getClass().getSimpleName());
            }
        }
    }

    public static class StringInfoWriter extends InfoWriter {

        public StringInfoWriter(final VCFInfoHeaderLine headerLine, final BCF2Encoder encoder, final int dictionaryOffset) {
            super(headerLine, encoder, dictionaryOffset);
        }

        @Override
        public void write(final VariantContext vc) throws IOException {
            final Object o = vc.getAttribute(key);
            if (o == null) {
                encoder.encodeTypedMissing(BCF2Type.CHAR);
            } else if (o instanceof String) {
                final byte[] bytes = ((String) o).getBytes(StandardCharsets.UTF_8);
                encoder.encodeTypedString(bytes);
            } else if (o instanceof List || o.getClass().isArray()) {
                final List<String> strings = o instanceof List
                    ? (List<String>) o
                    : Arrays.asList((String[]) o);
                final int observedValues = strings.size();
                final int nValues;
                if (!unbounded && observedValues > (nValues = headerLine.getCount(vc))) {
                    throw new TribbleException("Observed number of values: " + observedValues + " exceeds expected number: " + nValues);
                }
                final byte[] bytes = encoder.compactStrings(strings);
                encoder.encodeTypedString(bytes);
            } else {
                throw new TribbleException("Found attribute of incompatible type: " + o.getClass().getSimpleName());
            }
        }
    }

    /**
     * Base class for writers that encode FORMAT or genotype data.
     * <p>
     * This data may require two passes to encode as all genotype data must be of
     * uniform size and type, which may require one pass to determine. The uniform
     * size requirement also means genotype writers must pad their output to the size
     * of the largest value.
     */
    public static abstract class FormatWriter extends BCF2FieldWriter2 {

        protected BCF2Type type;

        protected int nValues;

        protected final String[] samples;

        public FormatWriter(final VCFFormatHeaderLine headerLine, final BCF2Encoder encoder, final int dictionaryOffset, final String[] samples) {
            super(headerLine, encoder, dictionaryOffset);
            this.samples = samples;
        }

        public final void encode(final VariantContext vc) throws IOException {
            encoder.encodeTypedInt(dictionaryOffset, dictionaryOffsetType);
            preprocess(vc);
            encoder.encodeType(nValues, type);
            write(vc);
        }

        /**
         * Determines type and size information for writers that require it
         */
        protected abstract void preprocess(final VariantContext vc);

        /**
         * Writes out the genotype information of the given VC
         */
        protected abstract void write(final VariantContext vc) throws IOException;
    }

    public static class AtomicIntFormatWriter extends FormatWriter {

        private final Integer[] values;

        public AtomicIntFormatWriter(final VCFFormatHeaderLine headerLine, final BCF2Encoder encoder, final int dictionaryOffset, final String[] samples) {
            super(headerLine, encoder, dictionaryOffset, samples);
            nValues = 1;
            values = new Integer[samples.length];
        }

        @Override
        protected void preprocess(final VariantContext vc) {
            type = BCF2Type.INT8;
            int i = 0;
            for (final String s : samples) {
                final Genotype g = vc.getGenotype(s);
                final Object o;
                if (g != null && (o = g.getExtendedAttribute(key)) != null) {
                    if (!(o instanceof Integer)) {
                        throw new TribbleException("Found attribute of incompatible type: " + o.getClass().getSimpleName());
                    }
                    final Integer v = (Integer) o;
                    values[i] = v;
                    type = BCF2Utils.maxIntegerType(type, BCF2Utils.determineIntegerType(v));
                } else {
                    values[i] = null;
                }
                i++;
            }
        }

        @Override
        protected void write(final VariantContext vc) throws IOException {
            for (final Integer v : values) {
                if (v == null) {
                    encoder.encodeRawMissingValue(type);
                } else {
                    encoder.encodeRawInt(v, type);
                }
            }
        }
    }

    public static class AtomicFloatFormatWriter extends FormatWriter {

        public AtomicFloatFormatWriter(final VCFFormatHeaderLine headerLine, final BCF2Encoder encoder, final int dictionaryOffset, final String[] samples) {
            super(headerLine, encoder, dictionaryOffset, samples);
            type = BCF2Type.FLOAT;
            nValues = 1;
        }

        @Override
        protected void preprocess(final VariantContext vc) {
            // Nothing to do for atomic floats
        }

        @Override
        protected void write(final VariantContext vc) throws IOException {
            for (final String s : samples) {
                final Genotype g = vc.getGenotype(s);
                final Object o;
                if (g != null && (o = g.getExtendedAttribute(key)) != null) {
                    if (!(o instanceof Double)) {
                        throw new TribbleException("Found attribute of incompatible type: " + o.getClass().getSimpleName());
                    }
                    encoder.encodeRawFloat((Double) o);
                } else {
                    // Either the context did not have a genotype with the given sample name
                    // or the attribute was missing, in either case we write missing
                    encoder.encodeRawMissingValue(BCF2Type.FLOAT);
                }
            }
        }
    }

    public static class CharFormatWriter extends FormatWriter {

        // Store byte array representations so we only create them once
        private final byte[][] strings;

        public CharFormatWriter(final VCFFormatHeaderLine headerLine, final BCF2Encoder encoder, final int dictionaryOffset, final String[] samples) {
            super(headerLine, encoder, dictionaryOffset, samples);
            strings = new byte[samples.length][];
            type = BCF2Type.CHAR;
        }

        @Override
        protected void preprocess(final VariantContext vc) {
            int observedValues = 0;
            int i = 0;
            for (final String s : samples) {
                final Genotype g = vc.getGenotype(s);
                final Object o;
                if (g != null && (o = g.getExtendedAttribute(key)) != null) {
                    if (o instanceof String) {
                        final byte[] bytes = ((String) o).getBytes(StandardCharsets.UTF_8);
                        strings[i] = bytes;
                        observedValues = Math.max(observedValues, bytes.length);
                    } else {
                        throw new TribbleException("Found attribute of incompatible type: " + o.getClass().getSimpleName());
                    }
                } else {
                    strings[i] = null;
                }
                i++;
            }

            if (unbounded) {
                nValues = observedValues;
            } else {
                nValues = headerLine.getCount(vc);
                if (observedValues > nValues)
                    throw new TribbleException("Observed number of values: " + observedValues + " exceeds expected number: " + nValues);
            }
        }

        @Override
        protected void write(final VariantContext vc) throws IOException {
            for (final byte[] s : strings) {
                if (s == null) {
                    encoder.encodeRawMissingValues(nValues, type);
                } else {
                    encoder.encodeRawString(s, nValues);
                }
            }
        }
    }

    public static class StringFormatWriter extends FormatWriter {

        // Store byte array representations so we only create them once
        private final byte[][] strings;

        public StringFormatWriter(final VCFFormatHeaderLine headerLine, final BCF2Encoder encoder, final int dictionaryOffset, final String[] samples) {
            super(headerLine, encoder, dictionaryOffset, samples);
            strings = new byte[samples.length][];
            type = BCF2Type.CHAR;
        }

        @Override
        protected void preprocess(final VariantContext vc) {
            // Number of strings seen, necessary for validation
            int observedValues = 0;

            int i = 0;
            for (final String s : samples) {
                final Genotype g = vc.getGenotype(s);
                final Object o;
                if (g != null && (o = g.getExtendedAttribute(key)) != null) {
                    final byte[] bytes;
                    if (o instanceof String) {
                        bytes = ((String) o).getBytes(StandardCharsets.UTF_8);
                        observedValues = Math.max(observedValues, 1);
                    } else if (o instanceof List || o.getClass().isArray()) {
                        final List<String> vs = o instanceof List
                            ? (List<String>) o
                            : Arrays.asList((String[]) o);
                        bytes = encoder.compactStrings(vs);
                        observedValues = Math.max(observedValues, vs.size());
                    } else {
                        throw new TribbleException("Found attribute of incompatible type: " + o.getClass().getSimpleName());
                    }
                    nValues = Math.max(nValues, bytes.length);
                    strings[i] = bytes;
                } else {
                    strings[i] = null;
                }
                i++;
            }

            if (!unbounded) {
                final int allowedStrings = headerLine.getCount(vc);
                if (observedValues > allowedStrings)
                    throw new TribbleException("Observed number of values: " + observedValues + " exceeds expected number: " + allowedStrings);
            }
        }

        @Override
        protected void write(final VariantContext vc) throws IOException {
            for (final byte[] s : strings) {
                if (s == null) {
                    encoder.encodeRawMissingValues(nValues, type);
                } else {
                    encoder.encodeRawString(s, nValues);
                }
            }
        }
    }

    public static class VecIntFormatWriter extends FormatWriter {

        // Max observed length of the actual vectors
        // If line count type is unbounded, the padding amount is value determined so we need to keep track of this
        private int maxLength;

        public VecIntFormatWriter(final VCFFormatHeaderLine headerLine, final BCF2Encoder encoder, final int dictionaryOffset, final String[] samples) {
            super(headerLine, encoder, dictionaryOffset, samples);
        }

        @Override
        protected void preprocess(final VariantContext vc) {
            maxLength = 0;
            type = BCF2Type.INT8;

            // Need to find narrowest integer width that fits all values
            for (final String s : samples) {
                final Genotype g = vc.getGenotype(s);
                final Object o;
                if (g != null && (o = vc.getAttribute(key)) != null) {
                    preprocess(o);
                }
            }

            if (unbounded) {
                nValues = maxLength;
            } else {
                nValues = headerLine.getCount(vc);
                if (maxLength > nValues)
                    throw new TribbleException("Observed number of values: " + maxLength + " exceeds expected number: " + nValues);
            }
        }

        private void preprocess(final Object o) {
            if (o instanceof List) {
                preprocess((List<Integer>) o);
            } else if (o instanceof Integer) {
                // Arrays that were pruned down to 1 element may be stored as the bare type instead of a List
                preprocess((Integer) o);
            } else if (o.getClass().isArray()) {
                preprocess((int[]) o);
            } else {
                throw new TribbleException("Found attribute of incompatible type: " + o.getClass().getSimpleName());
            }
        }

        private void preprocess(final List<Integer> vs) {
            maxLength = Math.max(maxLength, vs.size());
            for (final Integer v : vs) {
                if (v != null) {
                    type = BCF2Utils.maxIntegerType(type, BCF2Utils.determineIntegerType(v));
                    if (type == BCF2Type.INT32) return;
                }
            }
        }

        private void preprocess(final Integer v) {
            maxLength = Math.max(maxLength, 1);
            type = BCF2Utils.maxIntegerType(type, BCF2Utils.determineIntegerType(v));
        }

        private void preprocess(final int[] vs) {
            maxLength = Math.max(maxLength, vs.length);
            for (final int v : vs) {
                type = BCF2Utils.maxIntegerType(type, BCF2Utils.determineIntegerType(v));
                if (type == BCF2Type.INT32) return;
            }
        }

        @Override
        protected void write(final VariantContext vc) throws IOException {
            for (final String s : samples) {
                final Genotype g = vc.getGenotype(s);
                final Object o;
                if (g != null && (o = vc.getAttribute(key)) != null) {
                    if (o instanceof List) {
                        encoder.encodeRawVecInt((List<Integer>) o, nValues, type);
                    } else if (o instanceof Integer) {
                        encoder.encodeRawInt((Integer) o, type);
                        encoder.encodePaddingValues(nValues - 1, type);
                    } else if (o.getClass().isArray()) {
                        encoder.encodeRawVecInt((int[]) o, nValues, type);
                    } else {
                        throw new TribbleException("Found attribute of incompatible type: " + o.getClass().getSimpleName());
                    }
                } else {
                    encoder.encodePaddingValues(nValues, type);
                }
            }
        }
    }

    public static class VecFloatFormatWriter extends FormatWriter {

        private int maxLength;

        public VecFloatFormatWriter(final VCFFormatHeaderLine headerLine, final BCF2Encoder encoder, final int dictionaryOffset, final String[] samples) {
            super(headerLine, encoder, dictionaryOffset, samples);
            type = BCF2Type.FLOAT;
        }

        @Override
        protected void preprocess(final VariantContext vc) {
            maxLength = 0;
            for (final String s : samples) {
                final Genotype g = vc.getGenotype(s);
                final Object o;
                if (g != null && (o = vc.getAttribute(key)) != null) {
                    preprocess(o);
                }
            }

            if (unbounded) {
                nValues = maxLength;
            } else {
                nValues = headerLine.getCount(vc);
                if (maxLength > nValues)
                    throw new TribbleException("Observed number of values: " + maxLength + " exceeds expected number: " + nValues);
            }
        }

        private void preprocess(final Object o) {
            if (o instanceof List) {
                final List<Double> vs = (List<Double>) o;
                maxLength = Math.max(maxLength, vs.size());
            } else if (o instanceof Double) {
                maxLength = Math.max(maxLength, 1);
            } else if (o.getClass().isArray()) {
                final double[] vs = (double[]) o;
                maxLength = Math.max(maxLength, vs.length);
            } else {
                throw new TribbleException("Found attribute of incompatible type: " + o.getClass().getSimpleName());
            }
        }

        @Override
        protected void write(final VariantContext vc) throws IOException {
            for (final String s : samples) {
                final Genotype g = vc.getGenotype(s);
                final Object o;
                if (g != null && (o = vc.getAttribute(key)) != null) {
                    if (o instanceof List) {
                        encoder.encodeRawVecFLoat((List<Double>) o, nValues);
                    } else if (o.getClass().isArray()) {
                        encoder.encodeRawVecFLoat((double[]) o, nValues);
                    } else if (o instanceof Double) {
                        encoder.encodeRawFloat((Double) o);
                        encoder.encodePaddingValues(nValues - 1, type);
                    } else {
                        throw new TribbleException("Found attribute of incompatible type: " + o.getClass().getSimpleName());
                    }
                } else {
                    encoder.encodePaddingValues(nValues, type);
                }
            }
        }
    }

    /**
     * Base class for FORMAT writers that access genotype fields that are stored directly
     * as integer fields in the Genotype object as opposed to the attributes map.
     */
    private static abstract class InlineAtomicIntWriter extends FormatWriter {

        public InlineAtomicIntWriter(final VCFFormatHeaderLine headerLine, final BCF2Encoder encoder, final int dictionaryOffset, final String[] samples) {
            super(headerLine, encoder, dictionaryOffset, samples);
            if (headerLine.getType() != VCFHeaderLineType.Integer || headerLine.getCountType() != VCFHeaderLineCount.INTEGER || headerLine.getCount() != 1) {
                throw new TribbleException("Header line with standard key " + key + " has nonstandard type or count");
            }
            nValues = 1;
        }

        @Override
        protected void preprocess(final VariantContext vc) {
            type = BCF2Type.INT8;
            for (final String s : samples) {
                final Genotype g = vc.getGenotype(s);
                if (g == null) continue;
                final int v = get(g);
                type = BCF2Utils.maxIntegerType(type, BCF2Utils.determineIntegerType(v));
                if (type == BCF2Type.INT32) return;
            }
        }

        @Override
        protected void write(final VariantContext vc) throws IOException {
            for (final String s : samples) {
                final Genotype g = vc.getGenotype(s);
                if (g == null) {
                    encoder.encodeRawMissingValue(type);
                } else {
                    encoder.encodeRawInt(get(g), type);
                }
            }
        }

        protected abstract int get(final Genotype g);
    }

    public static class DPWriter extends InlineAtomicIntWriter {

        public DPWriter(final VCFFormatHeaderLine headerLine, final BCF2Encoder encoder, final int dictionaryOffset, final String[] samples) {
            super(headerLine, encoder, dictionaryOffset, samples);
        }

        @Override
        protected int get(final Genotype g) {
            return g.getDP();
        }
    }

    public static class GQWriter extends InlineAtomicIntWriter {

        public GQWriter(final VCFFormatHeaderLine headerLine, final BCF2Encoder encoder, final int dictionaryOffset, final String[] samples) {
            super(headerLine, encoder, dictionaryOffset, samples);
        }

        @Override
        protected int get(final Genotype g) {
            return g.getGQ();
        }
    }

    /**
     * Base class for FORMAT writers that access genotype fields that are stored directly
     * as integer array fields in the Genotype object as opposed to the attributes map.
     */
    private static abstract class InlineVecIntWriter extends FormatWriter {

        public InlineVecIntWriter(final VCFFormatHeaderLine headerLine, final BCF2Encoder encoder, final int dictionaryOffset, final String[] samples) {
            super(headerLine, encoder, dictionaryOffset, samples);
        }

        @Override
        protected void preprocess(final VariantContext vc) {
            type = BCF2Type.INT8;

            // For both vector of int types represented as inline fields by htsjdk (AD and PL),
            // the count type can be determined by inspecting the header
            nValues = headerLine.getCount(vc);

            // Find narrowest integer type that fits all values
            for (final String s : samples) {
                final Genotype g = vc.getGenotype(s);
                final int[] vs = get(g);

                if (vs == null) continue;
                if (vs.length > nValues)
                    throw new TribbleException("Observed number of values: " + vs.length + " exceeds expected number: " + nValues);
                for (final int v : vs) {
                    type = BCF2Utils.maxIntegerType(type, BCF2Utils.determineIntegerType(v));
                    if (type == BCF2Type.INT32) return;
                }
            }
        }

        @Override
        protected void write(final VariantContext vc) throws IOException {
            for (final String s : samples) {
                final Genotype g = vc.getGenotype(s);
                int written = 0;
                final int[] vs;
                if (g != null && (vs = get(g)) != null) {
                    for (final int v : vs) {
                        encoder.encodeRawInt(v, type);
                    }
                    written = vs.length;
                }
                encoder.encodePaddingValues(nValues - written, type);
            }
        }

        protected abstract int[] get(final Genotype g);
    }

    public static class ADWriter extends InlineVecIntWriter {

        public ADWriter(final VCFFormatHeaderLine headerLine, final BCF2Encoder encoder, final int dictionaryOffset, final String[] samples) {
            super(headerLine, encoder, dictionaryOffset, samples);
            if (headerLine.getType() != VCFHeaderLineType.Integer || headerLine.getCountType() != VCFHeaderLineCount.R) {
                throw new TribbleException("Header line with standard key AD has nonstandard type or count");
            }
        }

        @Override
        protected int[] get(final Genotype g) {
            return g.getAD();
        }
    }

    public static class PLWriter extends InlineVecIntWriter {

        public PLWriter(final VCFFormatHeaderLine headerLine, final BCF2Encoder encoder, final int dictionaryOffset, final String[] samples) {
            super(headerLine, encoder, dictionaryOffset, samples);
            if (headerLine.getType() != VCFHeaderLineType.Integer || headerLine.getCountType() != VCFHeaderLineCount.G) {
                throw new TribbleException("Header line with standard key PL has nonstandard type or count");
            }
        }

        @Override
        protected int[] get(final Genotype g) {
            return g.getPL();
        }
    }

    /**
     * Writer for the FT or filter field. This is a special case of the String/Character
     * writer, where the type of the value is known to be String (and not List<String>)
     * and null values must be specially handled by encoding them as PASS.
     */
// TODO should null always be interpreted as PASS? Is "no filters applied" reliably stored as non-null String "."?
    public static class FTWriter extends FormatWriter {

        private static final byte[] PASS = "PASS".getBytes(StandardCharsets.US_ASCII);

        private final byte[][] strings;

        public FTWriter(final VCFFormatHeaderLine headerLine, final BCF2Encoder encoder, final int dictionaryOffset, final String[] samples) {
            super(headerLine, encoder, dictionaryOffset, samples);
            if (headerLine.getType() != VCFHeaderLineType.String || headerLine.getCountType() != VCFHeaderLineCount.INTEGER || headerLine.getCount() != 1) {
                throw new TribbleException("Header line with standard key FT has nonstandard type or count");
            }

            type = BCF2Type.CHAR;
            strings = new byte[samples.length][];
        }

        @Override
        protected void preprocess(final VariantContext vc) {
            nValues = 0;
            // Need to determine longest string to pad out shorter strings to right size
            int i = 0;
            for (final String s : samples) {
                final Genotype g = vc.getGenotype(s);
                final String f;
                final byte[] bytes;
                if (g != null && (f = g.getFilters()) != null) {
                    bytes = f.getBytes(StandardCharsets.UTF_8);
                } else {
                    bytes = PASS;
                }
                strings[i] = bytes;
                nValues = Math.max(nValues, bytes.length);
                i++;
            }
        }

        @Override
        protected void write(final VariantContext vc) throws IOException {
            for (final byte[] s : strings)
                encoder.encodeRawString(s, nValues);
        }
    }

    /**
     * Specialized writer for GT field.
     */
    public static class GTWriter extends FormatWriter {
        private final HashMap<Allele, Integer> alleleMapForTriPlus = new HashMap<>(5);

        private Allele ref, alt1;

        public GTWriter(final VCFFormatHeaderLine headerLine, final BCF2Encoder encoder, final int dictionaryOffset, final String[] samples) {
            super(headerLine, encoder, dictionaryOffset, samples);
            if (headerLine.getType() != VCFHeaderLineType.String || headerLine.getCountType() != VCFHeaderLineCount.INTEGER || headerLine.getCount() != 1) {
                throw new TribbleException("Header line with standard key GT has nonstandard type or count");
            }
        }

        @Override
        protected void preprocess(final VariantContext vc) {
            buildAlleleMap(vc);
            nValues = vc.getMaxPloidy(2);
            // Ploidy should always fit into a signed 8-bit integer but do this check anyway for spec compliance
            type = BCF2Utils.determineIntegerType(nValues);
        }

        @Override
        protected void write(final VariantContext vc) throws IOException {
            for (final String s : samples) {
                final Genotype g = vc.getGenotype(s);
                if (g != null) {
                    for (final Allele a : g.getAlleles()) {
                        // we encode the actual allele
                        final int offset = getAlleleOffset(a);
                        final int encoded = ((offset + 1) << 1) | ((g.isPhased() && a != ref) ? 0x01 : 0x00);
                        encoder.encodeRawInt(encoded, type);
                    }
                    // Pad with missing values if sample ploidy is less than maximum
                    encoder.encodePaddingValues(nValues - g.getPloidy(), type);
                } else {
                    // Entirely missing genotype, which we encode as vector of no calls, or 0
                    for (int i = 0; i < nValues; i++) {
                        encoder.encodeRawInt(0, type);
                    }
                }
            }
        }

        /**
         * Fast path code to determine the offset.
         * <p>
         * Inline tests for == against ref (most common, first test)
         * == alt1 (second most common, second test)
         * == NO_CALL (third)
         * and finally in the map from allele => offset for all alt 2+ alleles
         *
         * @param a the allele whose offset we wish to determine
         * @return the offset (from 0) of the allele in the list of variant context alleles (-1 means NO_CALL)
         */
        private int getAlleleOffset(final Allele a) {
            if (a == ref) return 0;
            else if (a == alt1) return 1;
            else if (a == Allele.NO_CALL) return -1;
            else {
                final Integer o = alleleMapForTriPlus.get(a);
                if (o == null) throw new IllegalStateException("BUG: Couldn't find allele offset for allele " + a);
                return o;
            }
        }

        private void buildAlleleMap(final VariantContext vc) {
            // ref and alt1 are handled by a fast path when determining the offset
            // so they do not need to be placed in the map
            final int nAlleles = vc.getNAlleles();
            ref = vc.getReference();
            alt1 = nAlleles > 1 ? vc.getAlternateAllele(0) : null;

            if (nAlleles > 2) {
                // for multi-allelics we need to clear the map, and add additional looks
                alleleMapForTriPlus.clear();
                final List<Allele> alleles = vc.getAlleles();
                for (int i = 2; i < alleles.size(); i++) {
                    alleleMapForTriPlus.put(alleles.get(i), i);
                }
            }
        }
    }
}
