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
     * @param endpoint resource to request metadata for, without "/metadata" suffix
     */
    public RefgetMetadataRequest(final URI endpoint) {
        this.endpoint = URI.create(endpoint.toString() + "/metadata");
    }

    public RefgetMetadataResponse getResponse() {
        try {
            final HttpURLConnection conn = (HttpURLConnection) this.endpoint.toURL().openConnection();
            conn.connect();

            final InputStream is = conn.getInputStream();
            final int statusCode = conn.getResponseCode();

            final BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            final StringBuilder out = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                out.append(line);
            }
            final String json = out.toString();

            if (400 <= statusCode && statusCode < 500) {
                throw new IllegalArgumentException("Invalid request, received error code: " + statusCode);
            } else if (statusCode == 200) {
                return RefgetMetadataResponse.parse(Json.read(json));
            } else {
                throw new IllegalStateException("Unrecognized status code: " + statusCode);
            }
        } catch (final IOException e) {
            throw new RuntimeException("IOException while attempting refget metadata request: " + this.endpoint, e);
        }
    }
}
