package htsjdk.samtools.util.refget;

import mjson.Json;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;

/**
 * Class representing for a refget metadata request as defined in http://samtools.github.io/hts-specs/refget.html
 */
public class RefgetMetadataRequest {
    private final URI endpoint;

    /**
     * Construct a metadata request
     *
     * @param endpoint resource to request metadata for, with "/metadata" suffix
     */
    public RefgetMetadataRequest(final URI endpoint) {
        this.endpoint = endpoint;
    }

    public RefgetMetadataResponse getResponse() {
        final String json;
        final int status;
        final HttpURLConnection conn;

        try {
            conn = (HttpURLConnection) this.endpoint.toURL().openConnection();
            conn.setRequestProperty("Accept", "*/*");
            conn.connect();

            final InputStream is = conn.getInputStream();
            status = conn.getResponseCode();

            final BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            final StringBuilder out = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                out.append(line);
            }
            json = out.toString();
        } catch (final IOException e) {
            throw new RuntimeException("IOException while attempting refget metadata request: " + this.endpoint, e);
        }

        if (400 <= status && status < 500) {
            throw new IllegalArgumentException("Invalid request, received error code: " + status);
        } else if (status == 200) {
            return RefgetMetadataResponse.parse(Json.read(json));
        } else if (status == HttpURLConnection.HTTP_MOVED_TEMP
            || status == HttpURLConnection.HTTP_MOVED_PERM
            || status == HttpURLConnection.HTTP_SEE_OTHER) {
            final String redirect = conn.getHeaderField("Location");
            return new RefgetMetadataRequest(URI.create(redirect)).getResponse();
        } else {
            throw new IllegalStateException("Unrecognized status code: " + status);
        }
    }
}
