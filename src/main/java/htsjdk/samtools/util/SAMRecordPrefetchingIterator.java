package htsjdk.samtools.util;

import htsjdk.samtools.SAMRecord;

/**
 * Iterator that uses a dedicated background thread to prefetch SAMRecords,
 * reading ahead by a set number of bases to improve throughput.
 * <p>
 * Note that this implementation is not synchronized. If multiple threads
 * access an instance concurrently, it must be synchronized externally.
 */
public class SAMRecordPrefetchingIterator implements CloseableIterator<SAMRecord> {

    private static final class SAMRecordPrefetchingGuard implements AsyncPrefetchingIterator.Guard<SAMRecord> {
        private final int basePrefetchLimit;
        private int basesAllowed;

        public SAMRecordPrefetchingGuard(final int basePrefetchLimit) {
            this.basePrefetchLimit = basePrefetchLimit;
            this.basesAllowed = basePrefetchLimit;
        }

        @Override
        public boolean tryAcquire(final SAMRecord item) {
            final int bases = item.getReadLength();
            if (this.basesAllowed < bases && this.basesAllowed < this.basePrefetchLimit) {
                return false;
            } else {
                this.basesAllowed -= bases;
                return true;
            }
        }

        @Override
        public void release(final SAMRecord item) {
            this.basesAllowed += item.getReadLength();
        }

        public int readsInQueue() {
            return this.basePrefetchLimit - this.basesAllowed;
        }
    }

    private final SAMRecordPrefetchingGuard guard;
    private final AsyncPrefetchingIterator<SAMRecord> inner;

    /**
     * Creates a new iterator that traverses the given iterator on a background thread
     *
     * @param iterator          the iterator to traverse
     * @param basePrefetchLimit the number of bases to prefetch
     */
    public SAMRecordPrefetchingIterator(final CloseableIterator<SAMRecord> iterator, final int basePrefetchLimit) {
        this.guard = new SAMRecordPrefetchingGuard(basePrefetchLimit);
        this.inner = new AsyncPrefetchingIterator<>(iterator, this.guard);
    }

    @Override
    public void close() {
        this.inner.close();
    }

    @Override
    public boolean hasNext() {
        return this.inner.hasNext();
    }

    @Override
    public SAMRecord next() {
        return this.inner.next();
    }

    public int readsInQueue() {
        synchronized (this.guard) {
            return this.guard.readsInQueue();
        }
    }
}