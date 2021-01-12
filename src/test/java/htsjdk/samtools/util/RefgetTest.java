package htsjdk.samtools.util;

import htsjdk.HtsjdkTest;
import htsjdk.samtools.util.refget.*;
import mjson.Json;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.net.URI;
import java.util.List;

public class RefgetTest extends HtsjdkTest {
    private static final URI testSequence1 = URI.create("https://www.ebi.ac.uk/ena/cram/sequence/3050107579885e1608e6fe50fae3f8d0");
    private static final String contents = "TAGCGGGCCTTGTATCTTTTAGAC";

//    private static final URI testSequence1 = URI.create("https://refget-insdc.jeremy-codes.com/sequence/2085c82d80500a91dd0b8aa9237b0e43f1c07809bd6e6785");
//    private static final String contents = "TTCTCAATCCCCAATGCTTGGCTTCCATAAGCAGATGGATAA";


    private Json wrapMetadata(final Json j) {
        return Json.object("metadata", j);
    }

    @Test
    public void testDeserializeMetadataResponse() {
        final Json metadata = Json.object();
        metadata.set("md5", "abc");
        metadata.set("TRUNC512", "def");
        metadata.set("length", 2);
        metadata.set("aliases", Json.array(
            Json.object("alias", "alias0", "naming_authority", "authority0"),
            Json.object("alias", "alias1", "naming_authority", "authority1")));

        final RefgetMetadataResponse resp = RefgetMetadataResponse.parse(wrapMetadata(metadata));
        Assert.assertEquals(resp.getMd5(), "abc");
        Assert.assertEquals(resp.getTRUNC512(), "def");
        Assert.assertEquals(resp.getLength(), 2);

        final List<RefgetMetadataResponse.Alias> aliases = resp.getAliases();
        Assert.assertEquals(aliases.get(0).getAlias(), "alias0");
        Assert.assertEquals(aliases.get(1).getAlias(), "alias1");
        Assert.assertEquals(aliases.get(0).getNamingAuthority(), "authority0");
        Assert.assertEquals(aliases.get(1).getNamingAuthority(), "authority1");
    }

    @DataProvider(name = "malformedMetadataResponseProvider")
    public Object[][] malformedMetadataResponseProvider() {
        final Json noMd5 = wrapMetadata(Json.object("TRUNC512", "abc", "length", 2, "aliases", Json.array()));
        final Json noTRUNC512 = wrapMetadata(Json.object("md5", "abc", "length", 2, "aliases", Json.array()));
        final Json noLength = wrapMetadata(Json.object("md5", "abc", "TRUNC512", "def", "aliases", Json.array()));
        final Json noAliases = wrapMetadata(Json.object("md5", "abc", "TRUNC512", "def", "length", 2));
        final Json missingAlias = wrapMetadata(Json.object("md5", "abc", "TRUNC512", "def", "length", 2, "aliases", Json.array(
            Json.object("naming_authority", "authority")
        )));
        final Json missingAuthority = wrapMetadata(Json.object("md5", "abc", "TRUNC512", "def", "length", 2, "aliases", Json.array(
            Json.object("alias", "alias")
        )));

        return new Object[][]{
            new Object[]{noMd5},
            new Object[]{noTRUNC512},
            new Object[]{noLength},
            new Object[]{noAliases},
            new Object[]{missingAlias},
            new Object[]{missingAuthority},
        };
    }

    @Test(dataProvider = "malformedMetadataResponseProvider", expectedExceptions = RefgetMalformedResponseException.class)
    public void testDeserializeMetadataMalformed(final Json j) {
        RefgetMetadataResponse.parse(j);
    }

    private Json wrapService(final Json j) {
        return Json.object("service", j);
    }

    @Test
    public void testDeserializeServiceInfoResponse() {
        final Json service = Json.object();
        service.set("circular_supported", true);
        service.set("algorithms", Json.array("md5", "TRUNC512"));
        service.set("subsequence_limit", Json.nil());
        service.set("supported_api_versions", Json.array("v1", "v2"));

        // Test with subsequence_limit field null
        RefgetServiceInfoResponse resp = RefgetServiceInfoResponse.parse(wrapService(service));
        Assert.assertTrue(resp.isCircularSupported());
        Assert.assertEquals(resp.getAlgorithms().get(0), "md5");
        Assert.assertEquals(resp.getAlgorithms().get(1), "TRUNC512");
        Assert.assertNull(resp.getSubsequenceLimit());
        Assert.assertEquals(resp.getSupportedVersions().get(0), "v1");
        Assert.assertEquals(resp.getSupportedVersions().get(1), "v2");

        // Test with subsequence_limit field set
        service.set("subsequence_limit", 2);
        resp = RefgetServiceInfoResponse.parse(wrapService(service));
        Assert.assertEquals(resp.getSubsequenceLimit(), Integer.valueOf(2));
    }

    @DataProvider(name = "malformedServiceInfoResponseProvider")
    public Object[][] malformedServiceInfoResponseProvider() {
        final Json noCircular = wrapService(Json.object("algorithms", Json.array(), "subsequence_limit", 2, "supported_api_versions", Json.array()));
        final Json noAlgorithms = wrapService(Json.object("circular_supported", false, "subsequence_limit", 2, "supported_api_versions", Json.array()));
        final Json noLimit = wrapService(Json.object("circular_supported", false, "algorithms", Json.array(), "supported_api_versions", Json.array()));
        final Json noAPIs = wrapService(Json.object("circular_supported", false, "subsequence_limit", 2, "algorithms", Json.array()));

        return new Object[][]{
            new Object[]{noCircular},
            new Object[]{noAlgorithms},
            new Object[]{noLimit},
            new Object[]{noAPIs},
        };
    }

    @Test(dataProvider = "malformedServiceInfoResponseProvider", expectedExceptions = RefgetMalformedResponseException.class)
    public void testDeserializeServiceInfoMalformed(final Json j) {
        RefgetServiceInfoResponse.parse(j);
    }

    @DataProvider(name = "refgetSequenceRequestProvider")
    public Object[][] refgetSequenceRequestProvider() {
        return new Object[][]{
            new Object[]{new RefgetSequenceRequest(testSequence1), contents},
            new Object[]{new RefgetSequenceRequest(testSequence1, 5), contents.substring(5)},
            new Object[]{new RefgetSequenceRequest(testSequence1, 5, 8), contents.substring(5, 8+1)},
        };
    }

    @Test(dataProvider = "refgetSequenceRequestProvider")
    public void testRefgetSequenceRequest(final RefgetSequenceRequest req, final String expected) {
        final InputStream input = req.getResponse();
        final String s = IOUtil.readFully(input).substring(0, expected.length());
        Assert.assertEquals(s, expected);
    }

    @Test
    public void testRefgetMetadataRequest() {
        final RefgetMetadataRequest req = new RefgetMetadataRequest(testSequence1);
        final RefgetMetadataResponse resp = req.getResponse();

        Assert.assertEquals(resp.getLength(), 5386);
        Assert.assertTrue(resp.getAliases().isEmpty());
        Assert.assertEquals(resp.getTRUNC512(), "2085c82d80500a91dd0b8aa9237b0e43f1c07809bd6e6785");
        Assert.assertEquals(resp.getMd5(), "3332ed720ac7eaa9b3655c06f6b9e196");
    }

    @Test
    public void testServiceInfoRequest() {
        final RefgetServiceInfoRequest req = new RefgetServiceInfoRequest(URI.create("https://www.ebi.ac.uk/ena/cram/sequence"));
        final RefgetServiceInfoResponse resp = req.getResponse();

        Assert.assertFalse(resp.isCircularSupported());
        Assert.assertEquals(resp.getAlgorithms().size(), 1);
        Assert.assertEquals(resp.getAlgorithms().get(0), "md5");
        Assert.assertNull(resp.getSubsequenceLimit());
        Assert.assertEquals(resp.getSupportedVersions().size(), 1);
        Assert.assertEquals(resp.getSupportedVersions().get(0), "0.2.0");
    }
}
