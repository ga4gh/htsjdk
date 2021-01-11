package htsjdk.samtools.util.refget;

public class RefgetMalformedResponseException extends RuntimeException {
    public RefgetMalformedResponseException() {}

    public RefgetMalformedResponseException(final String s) {
        super(s);
    }

    public RefgetMalformedResponseException(final String s, final Throwable throwable) {
        super(s, throwable);
    }

    public RefgetMalformedResponseException(final Throwable throwable) {
        super(throwable);
    }
}
