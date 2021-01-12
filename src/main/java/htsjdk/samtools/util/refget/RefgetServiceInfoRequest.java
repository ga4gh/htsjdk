package htsjdk.samtools.util.refget;

import mjson.Json;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;

public class RefgetServiceInfoRequest {
    private final URI endpoint;

    /**
     * Construct a service info request
     * @param endpoint resource to request service info for, with "/service-info" suffix
     */
    public RefgetServiceInfoRequest(final URI endpoint) {
        this.endpoint = endpoint;
    }

    public RefgetServiceInfoResponse getResponse() {
        final HttpURLConnection conn;
        final int status;
        final String json;
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
            throw new RuntimeException("IOException while attempting refget service info request: " + this.endpoint, e);
        }

        if (400 <= status && status < 500) {
            throw new IllegalArgumentException("Invalid request, received error code: " + status);
        } else if (200 <= status && status < 300) {
            return RefgetServiceInfoResponse.parse(Json.read(json));
        } else if (status == HttpURLConnection.HTTP_MOVED_TEMP
            || status == HttpURLConnection.HTTP_MOVED_PERM
            || status == HttpURLConnection.HTTP_SEE_OTHER) {
            final URI redirect = URI.create(conn.getHeaderField("location"));
            return new RefgetServiceInfoRequest(redirect).getResponse();
        } else {
            throw new IllegalStateException("Unrecognized status code: " + status);
        }
    }
}
