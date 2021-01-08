/*
 * The MIT License
 *
 * Copyright (c) 2017 The Broad Institute
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package htsjdk.samtools;

import htsjdk.HtsjdkTest;
import htsjdk.samtools.util.Interval;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Stream;

/**
 * Test for SAMReadGroupRecordTest
 */
public class SAMSequenceRecordTest extends HtsjdkTest {

    @Test
    public void testGetSAMString() {
        final SAMSequenceRecord r = new SAMSequenceRecord("chr5_but_without_a_prefix", 271828);
        r.setSpecies("Psephophorus terrypratchetti");
        r.setAssembly("GRCt01");
        r.setMd5("7a6dd3d307de916b477e7bf304ac22bc");
        Assert.assertEquals("@SQ\tSN:chr5_but_without_a_prefix\tLN:271828\tSP:Psephophorus terrypratchetti\tAS:GRCt01\tM5:7a6dd3d307de916b477e7bf304ac22bc", r.getSAMString());
    }

    @Test
    public void testLocatable() {
        final SAMSequenceRecord r = new SAMSequenceRecord("1", 100);
        Assert.assertTrue(r.overlaps(r));
        Assert.assertEquals(r.getStart(),1);
        Assert.assertEquals(r.getEnd(),r.getSequenceLength());
        Assert.assertEquals(r.getLengthOnReference(),r.getSequenceLength());
        Assert.assertTrue(r.overlaps(new Interval(r.getContig(), 50, 150)));
        Assert.assertFalse(r.overlaps(new Interval(r.getContig(), 101, 101)));
    }

    @DataProvider
    public Object[][] testIsSameSequenceData() {
        final SAMSequenceRecord rec1 = new SAMSequenceRecord("chr1", 100);
        final SAMSequenceRecord rec2 = new SAMSequenceRecord("chr2", 101);
        final SAMSequenceRecord rec3 = new SAMSequenceRecord("chr3", 0);
        final SAMSequenceRecord rec4 = new SAMSequenceRecord("chr1", 100);

        final String md5One = "1";
        final String md5Two = "2";
        final int index1 = 1;
        final int index2 = 2;

        return new Object[][]{
                new Object[]{rec1, rec1, md5One, md5One, index1, index1, true},
                new Object[]{rec1, null, md5One, md5One, index1, index1, false},
                new Object[]{rec1, rec4, md5One, md5One, index1, index1, true},
                new Object[]{rec1, rec4, md5One, md5One, index1, index2, false},
                new Object[]{rec1, rec3, md5One, md5Two, index1, index1, false},
                new Object[]{rec1, rec2, md5One, md5Two, index1, index1, false},
                new Object[]{rec1, rec4, md5One, null, index1, index1, true},
                new Object[]{rec1, rec4, null, md5One, index1, index1, true},
                new Object[]{rec1, rec4, md5One, md5One, index1, index2, false}
        };
    }

    @Test(dataProvider = "testIsSameSequenceData")
    public void testIsSameSequence(final SAMSequenceRecord rec1 , final SAMSequenceRecord rec2, final String md5One, final String md5Two,
                                   final int index1, final int index2, final boolean isSame) {
        if (rec2 != null) {
            rec2.setMd5(md5Two);
            rec2.setSequenceIndex(index2);
        }

        if (rec1 != null) {
            rec1.setMd5(md5One);
            rec1.setSequenceIndex(index1);
            Assert.assertEquals(rec1.isSameSequence(rec2), isSame);
        }
    }

    @DataProvider
    Object[][] testAccessFileWithAlternateContigNameData() {
        return new Object[][]{
                new Object[]{"chr1", 4},
                new Object[]{"1", 4},
                new Object[]{"chr2", 3},
                new Object[]{"chr3", 2},
                new Object[]{"3", 2},
        };
    }

    File AccessFileWithAlternateContigName;

    @BeforeTest
    void setup() throws IOException {
        File input = new File("src/test/resources/htsjdk/samtools/SamSequenceRecordTest/alternate_contig_names.sam");

        final File outputFile = File.createTempFile("tmp.", ".bam");
        outputFile.deleteOnExit();

        final SamReader samReader = SamReaderFactory.make().open(input);
        final SAMFileHeader fileHeader = samReader.getFileHeader().clone();
        fileHeader.setSortOrder(SAMFileHeader.SortOrder.coordinate);

        try (SAMFileWriter samWriter = new SAMFileWriterFactory().setCreateIndex(true).makeWriter(fileHeader, false, outputFile, null)) {
            samReader.forEach(samWriter::addAlignment);
        }
        AccessFileWithAlternateContigName = outputFile;
    }

    @Test(dataProvider = "testAccessFileWithAlternateContigNameData")
    public void testAccessFileWithAlternateContigName(final String contigName, final int expectedRecords) throws IOException {
        try(SamReader bamReader = SamReaderFactory.make().open(AccessFileWithAlternateContigName);
            Stream<SAMRecord> recordStream = bamReader.query(contigName, 1, 101, false).stream()) {
            Assert.assertEquals(recordStream.count(), expectedRecords);
        }
    }

    @Test
    public void testAlternativeSequences() {
        final SAMSequenceRecord chr1 = new SAMSequenceRecord("1", 100);

        // no AN tag yet
        Assert.assertFalse(chr1.hasAlternativeSequenceNames());
        Assert.assertTrue(chr1.getAlternativeSequenceNames().isEmpty());

        // set to a random alias
        chr1.addAlternativeSequenceName("my-chromosome");
        Assert.assertNotEquals(chr1, new SAMSequenceRecord(chr1.getSequenceName(), chr1.getSequenceLength()));
        Assert.assertTrue(chr1.hasAlternativeSequenceNames());
        Assert.assertEquals(chr1.getAlternativeSequenceNames(), Collections.singleton("my-chromosome"));
        Assert.assertEquals("@SQ\tSN:1\tLN:100\tAN:my-chromosome", chr1.getSAMString());

        // set to new chromosome aliases (removing previous)
        final List<String> chr1AltNames = Arrays.asList("chr1","chr01","01","CM000663");
        chr1.setAlternativeSequenceName(chr1AltNames);
        Assert.assertTrue(chr1.hasAlternativeSequenceNames());
        Assert.assertEquals( chr1.getAlternativeSequenceNames(), new HashSet<>(chr1AltNames));

        //alt names are sorted now
        Assert.assertEquals(chr1.getSAMString(), "@SQ\tSN:1\tLN:100\tAN:01,CM000663,chr01,chr1");
    }



    @DataProvider
    public Object[][] validAlternativeSequences() {
        return new Object[][] {
                // only characters
                {"a"},
                {"alias"},
                // valid symbols after first character ('*', '+', '.', '@', '_', '|', '-')
                {"my*contig"},
                {"my+contig"},
                {"my.contig"},
                {"my@contig"},
                {"my_contig"},
                {"my|contig"},
                {"my-contig"}
        };
    }

    @Test(dataProvider = "validAlternativeSequences")
    public void testValidAlternativeSequences(final String altName) {
        final SAMSequenceRecord contig = new SAMSequenceRecord("contig", 100);
        // should not throw
        contig.setAlternativeSequenceName(Collections.singleton(altName));
        Assert.assertTrue(contig.hasAlternativeSequenceNames());
        Assert.assertEquals(contig.getAlternativeSequenceNames(), Collections.singleton(altName));
    }

    @Test(dataProvider = "illegalSequenceNames")
    public void testInvalidAlternativeSequences(final String altName) {
        final SAMSequenceRecord chr1 = new SAMSequenceRecord("1", 100);
        Assert.assertThrows(IllegalArgumentException.class, () -> chr1.addAlternativeSequenceName(altName));
        Assert.assertThrows(IllegalArgumentException.class, () -> chr1.setAlternativeSequenceName(Collections.singleton(altName)));
    }

    @Test
    public void testSetAndCheckDescription() {
        final SAMSequenceRecord record = new SAMSequenceRecord("Test", 1000);
        Assert.assertNull(record.getDescription());
        final String description = "A description.";
        record.setDescription(description);
        Assert.assertEquals(record.getDescription(), description);
    }

    @DataProvider
    public Object[][] illegalSequenceNames() {
        return new Object[][]{
                {",chr1"},
                // comma-separated
                {"chr1,alt"},
                {"space "},
                {"comma,"},
                {"lbrace["},
                {"rbrace]"},
                {"slash\\"},
                {"smaller<"},
                {"bigger<"},
                {"lparen("},
                {"rparen)"},
                {"lbracket{"},
                {"rbracket}"},
                {""}
        };
    }

    @Test(dataProvider = "illegalSequenceNames", expectedExceptions = SAMException.class)
    public void testIllegalSequenceNames(final String sequenceName){
        new SAMSequenceRecord(sequenceName,100);
    }
}
