package htsjdk.samtools.util.refget;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;

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
     * Construct a request with only a start and no end
     *
     * @param endpoint complete URI of refget resource including ID
     * @param start    start of request range, 0-based, inclusive
     */
    public RefgetSequenceRequest(final URI endpoint, final int start) {
        this.endpoint = endpoint;
        this.startInclusive = start;
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
        try {
            final HttpURLConnection conn = (HttpURLConnection) this.endpoint.toURL().openConnection();
            if (this.startInclusive != null) {
                final StringBuilder s = new StringBuilder("bytes=");
                s.append(this.startInclusive);
                if (this.endInclusive != null) {
                    s.append('-');
                    s.append(this.endInclusive.toString());
                }
                conn.setRequestProperty("Range", s.toString());
            }

            conn.connect();
            return conn.getInputStream();
        } catch (final IOException e) {
            throw new RuntimeException("IOException while attempting refget sequence request: " + this.endpoint, e);
        }
    }
}
