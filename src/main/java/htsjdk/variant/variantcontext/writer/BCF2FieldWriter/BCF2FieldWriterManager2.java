package htsjdk.variant.variantcontext.writer.BCF2FieldWriter;

import htsjdk.variant.variantcontext.writer.BCF2Encoder;
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFInfoHeaderLine;

import java.util.HashMap;
import java.util.Map;

public class BCF2FieldWriterManager2 {

    private final Map<String, BCF2FieldWriter.SiteWriter> infoWriters;
    private final Map<String, BCF2FieldWriter.GenotypeWriter> formatWriters;

    public BCF2FieldWriterManager2(final VCFHeader header, final Map<String, Integer> dict, final BCF2Encoder encoder) {
        // TODO this would be a good place to validate that header lines with standard keys
        //  match the type and count declared in the spec
        infoWriters = new HashMap<>(header.getInfoHeaderLines().size());
        for (final VCFInfoHeaderLine line : header.getInfoHeaderLines()) {
            final String field = line.getID();
            final int offset = dict.get(field);
            final BCF2FieldWriter.SiteWriter writer = BCF2FieldWriter.createSiteWriter(line, offset, encoder);
            add(infoWriters, field, writer);
        }

        formatWriters = new HashMap<>(header.getFormatHeaderLines().size());
        for (final VCFFormatHeaderLine line : header.getFormatHeaderLines()) {
            final String field = line.getID();
            final int offset = dict.get(field);
            final BCF2FieldWriter.GenotypeWriter writer = BCF2FieldWriter.createGenotypeWriter(line, offset, encoder);
            add(formatWriters, field, writer);
        }
    }

    public BCF2FieldWriter.SiteWriter getInfoWriter(final String field) {
        return infoWriters.get(field);
    }

    public BCF2FieldWriter.GenotypeWriter getFormatWriter(final String field) {
        return formatWriters.get(field);
    }

    private <T> void add(final Map<String, T> map, final String field, final T writer) {
        if (map.containsKey(field))
            throw new IllegalStateException("BUG: field " + field + " already seen in VCFHeader while building BCF2 field encoders");
        map.put(field, writer);
    }
}
