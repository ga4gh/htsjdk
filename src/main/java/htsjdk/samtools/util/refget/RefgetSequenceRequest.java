package htsjdk.samtools.util.refget;

import htsjdk.samtools.util.IOUtil;
import org.apache.commons.compress.utils.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.stream.Collectors;

/**
 * Class that allows making a sequence request against a specified refget server
 */
public class RefgetSequenceRequest {
    private final URI endpoint;
    private Integer startInclusive;
    private Integer endInclusive;

    /**
     * Construct a request with no start or end
     *
     * @param endpoint complete URI of refget resource including ID
     */
    public RefgetSequenceRequest(final URI endpoint) {
        this.endpoint = endpoint;
    }

    /**
     * Construct a request with both start and end
     *
     * @param endpoint complete URI of refget resource including ID
     * @param start    start of request range, 0-based, inclusive
     * @param end      end of request range, 0-based, inclusive
     */
    public RefgetSequenceRequest(final URI endpoint, final int start, final int end) {
        this.endpoint = endpoint;
        this.startInclusive = start;
        this.endInclusive = end;
    }

    /**
     * Attempt to make refget sequence request and return response if there are no errors
     *
     * @return the response from the refget server if request is successful
     */
    public InputStream getResponse() {
        final HttpURLConnection conn;
        try {
            conn = (HttpURLConnection) this.endpoint.toURL().openConnection();
            if (this.startInclusive != null) {
                final String s = "bytes=" + this.startInclusive + '-' + this.endInclusive;
                conn.setRequestProperty("Range", s);
            }

            conn.setRequestProperty("Accept", "*/*");
            conn.connect();

            final int status = conn.getResponseCode();
            if (status == HttpURLConnection.HTTP_MOVED_TEMP
                || status == HttpURLConnection.HTTP_MOVED_PERM
                || status == HttpURLConnection.HTTP_SEE_OTHER) {
                final URI redirect = URI.create(conn.getHeaderField("Location"));
                final RefgetSequenceRequest req = this.startInclusive == null
                    ? new RefgetSequenceRequest(redirect)
                    : new RefgetSequenceRequest(redirect, startInclusive, endInclusive);
                return req.getResponse();
            }
            return conn.getInputStream();
        } catch (final IOException e) {
            throw new RuntimeException("IOException while attempting refget sequence request: " + this.endpoint, e);
        }
    }
}
