package htsjdk.variant.variantcontext.writer;

import htsjdk.tribble.TribbleException;
import htsjdk.variant.vcf.VCFConstants;
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLineCount;
import htsjdk.variant.vcf.VCFInfoHeaderLine;

import java.util.HashMap;
import java.util.Map;

public class BCF2FieldWriterManager2 {

    private final Map<String, BCF2FieldWriter2.InfoWriter> infoWriters;
    private final Map<String, BCF2FieldWriter2.FormatWriter> formatWriters;

    public BCF2FieldWriterManager2(final VCFHeader header, final Map<String, Integer> dict, final BCF2Encoder encoder, final String[] samples) {
        infoWriters = new HashMap<>(header.getInfoHeaderLines().size());
        for (final VCFInfoHeaderLine line : header.getInfoHeaderLines()) {
            final String field = line.getID();
            final int offset = dict.get(field);
            final BCF2FieldWriter2.InfoWriter writer = createInfoWriter(line, encoder, offset);
            add(infoWriters, field, writer);
        }

        formatWriters = new HashMap<>(header.getFormatHeaderLines().size());
        for (final VCFFormatHeaderLine line : header.getFormatHeaderLines()) {
            final String field = line.getID();
            final int offset = dict.get(field);
            final BCF2FieldWriter2.FormatWriter writer = createFormatWriter(line, encoder, offset, samples);
            add(formatWriters, field, writer);
        }
    }

    public BCF2FieldWriter2.InfoWriter getInfoWriter(final String field) {
        return infoWriters.get(field);
    }

    public BCF2FieldWriter2.FormatWriter getFormatWriter(final String field) {
        return formatWriters.get(field);
    }

    private BCF2FieldWriter2.InfoWriter createInfoWriter(
        final VCFInfoHeaderLine line,
        final BCF2Encoder encoder,
        final int offset
    ) {
        switch (line.getType()) {
            case Integer:
                return line.getCountType() == VCFHeaderLineCount.INTEGER && line.getCount() == 1
                    ? new BCF2FieldWriter2.AtomicIntInfoWriter(line, encoder, offset)
                    : new BCF2FieldWriter2.VecIntInfoWriter(line, encoder, offset);
            case Float:
                return line.getCountType() == VCFHeaderLineCount.INTEGER && line.getCount() == 1
                    ? new BCF2FieldWriter2.AtomicFloatInfoWriter(line, encoder, offset)
                    : new BCF2FieldWriter2.VecFloatInfoWriter(line, encoder, offset);
            case Flag:
                return new BCF2FieldWriter2.FlagInfoWriter(line, encoder, offset);
            case String:
                return new BCF2FieldWriter2.StringInfoWriter(line, encoder, offset);
            case Character:
                return new BCF2FieldWriter2.CharInfoWriter(line, encoder, offset);
            default:
                throw new TribbleException("Unrecognized line type: " + line.getType());
        }
    }

    private BCF2FieldWriter2.FormatWriter createFormatWriter(
        final VCFFormatHeaderLine line,
        final BCF2Encoder encoder,
        final int offset,
        final String[] samples
    ) {
        // Specialized writers for fields stored inline in the Genotype and not in its attributes map
        switch (line.getID()) {
            case VCFConstants.GENOTYPE_KEY:
                return new BCF2FieldWriter2.GTWriter(line, encoder, offset, samples);
            case VCFConstants.GENOTYPE_FILTER_KEY:
                return new BCF2FieldWriter2.FTWriter(line, encoder, offset, samples);
            case VCFConstants.DEPTH_KEY:
                return new BCF2FieldWriter2.DPWriter(line, encoder, offset, samples);
            case VCFConstants.GENOTYPE_QUALITY_KEY:
                return new BCF2FieldWriter2.GQWriter(line, encoder, offset, samples);
            case VCFConstants.GENOTYPE_ALLELE_DEPTHS:
                return new BCF2FieldWriter2.ADWriter(line, encoder, offset, samples);
            case VCFConstants.GENOTYPE_PL_KEY:
                return new BCF2FieldWriter2.PLWriter(line, encoder, offset, samples);
        }

        switch (line.getType()) {
            case Integer:
                return line.getCountType() == VCFHeaderLineCount.INTEGER && line.getCount() == 1
                    ? new BCF2FieldWriter2.AtomicIntFormatWriter(line, encoder, offset, samples)
                    : new BCF2FieldWriter2.VecIntFormatWriter(line, encoder, offset, samples);
            case Float:
                return line.getCountType() == VCFHeaderLineCount.INTEGER && line.getCount() == 1
                    ? new BCF2FieldWriter2.AtomicFloatFormatWriter(line, encoder, offset, samples)
                    : new BCF2FieldWriter2.VecFloatFormatWriter(line, encoder, offset, samples);
            case Flag:
                throw new TribbleException("Format lines cannot have type Flag");
            case String:
                return new BCF2FieldWriter2.StringFormatWriter(line, encoder, offset, samples);
            case Character:
                return new BCF2FieldWriter2.CharFormatWriter(line, encoder, offset, samples);
            default:
                throw new TribbleException("Unrecognized line type: " + line.getType());
        }
    }

    private <T> void add(final Map<String, T> map, final String field, final T writer) {
        if (map.containsKey(field))
            throw new IllegalStateException("BUG: field " + field + " already seen in VCFHeader while building BCF2 field encoders");
        map.put(field, writer);
    }
}
