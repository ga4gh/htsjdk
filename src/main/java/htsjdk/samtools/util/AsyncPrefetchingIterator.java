package htsjdk.samtools.util;

import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Iterator that uses a dedicated background thread to prefetch items from a wrapped iterator.
 * <p>
 * This class accepts a user provided {@link Guard} object functioning like a semaphore, which can contain arbitrary
 * logic to control the rate at which items are fetched off the inner iterator.
 * <p>
 * Note that this implementation is not synchronized. If multiple threads
 * access an instance concurrently, it must be synchronized externally.
 */
public class AsyncPrefetchingIterator<T> implements CloseableIterator<T> {

    /**
     * An interface for the guard used in {@link AsyncPrefetchingIterator} which can contain arbitrary logic to control
     * the rate at which items are fetched off the inner iterator.
     * <p>
     * The implementations of this class do not need to be synchronized, as synchronization is handled by the iterator.
     *
     * @param <U> the type of the AsyncPrefetchingIterator this guard limits the prefetching rate of
     */
    public interface Guard<U> {
        /**
         * This method will be called at least once for each item that is taken off the inner iterator, until the guard
         * implementation determines that the item should be allowed through the AsyncPrefetchingIterator
         * <p>
         * If the item should be allowed through and the prefetcher allowed to continue, this method should update the
         * guard's state to reflect that the item has been allowed, then return true, otherwise it should return false
         * without mutating the guard, which will stop the prefetcher until items are removed from the iterator
         * and the guard determines the item can be allowed through.
         * <p>
         * This method should never block.
         * <p>
         *
         * @param item the item to try to allow through
         */
        boolean tryAcquire(final U item);

        /**
         * This method will be called exactly once for each item that is returned by #{@link #next()}
         * <p>
         * It should update the guard to reflect that the item is no longer in its queue.
         *
         * @param item the item to release
         */
        void release(final U item);
    }

    /**
     * Internal type with either one item from inner iterator or an exception but not both.
     * <p>
     * Used to propagate errors back to the thread consuming from the iterator at the point
     * where the exception would have occurred if the inner iterator were being read sequentially.
     */
    private class Either {
        private final T item;
        private final Throwable error;

        public Either(final T item) {
            this.item = item;
            this.error = null;
        }

        public Either(final Throwable error) {
            this.item = null;
            this.error = error;
        }
    }

    private final PeekableIterator<T> inner;
    private final BlockingQueue<AsyncPrefetchingIterator<T>.Either> queue;
    private final Guard<? super T> guard;
    private Thread backgroundThread;

    /**
     * Construct a new AsyncPrefetchingIterator reading from the given iterator and guard
     *
     * @param iterator inner iterator to be read from
     * @param guard    guard limiting the rate at which items are taken off the inner iterator
     */
    public AsyncPrefetchingIterator(final CloseableIterator<T> iterator, final Guard<? super T> guard) {
        this.inner = new PeekableIterator<>(iterator);
        this.guard = guard;
        this.queue = new LinkedBlockingQueue<>();

        this.backgroundThread = new Thread(this::prefetch, SAMRecordPrefetchingIterator.class.getSimpleName() + "Thread");
        this.backgroundThread.setDaemon(true);
        this.backgroundThread.start();
    }

    private void prefetch() {
        while (this.inner.hasNext() && !Thread.currentThread().isInterrupted()) {
            final T next = this.inner.peek();
            try {
                synchronized (this.guard) {
                    while (!this.guard.tryAcquire(next)) {
                        if (this.backgroundThread.isInterrupted()) {
                            return;
                        }
                        this.guard.wait();
                    }
                }

                // Synchronized to prevent race condition where last item is taken off inner iterator
                // then there is a context switch and hasNext() is called before the item is placed onto the queue
                synchronized (this) {
                    this.inner.next();
                    this.queue.add(new AsyncPrefetchingIterator<T>.Either(next));
                }
            } catch (final InterruptedException e) {
                // InterruptedException is expected if the iterator is being closed
                return;
            } catch (final Throwable t) {
                // All other exceptions are placed onto the queue so they can be reported when accessed by the main thread
                // Errors are immediately printed so their information is propagated to the user and not lost
                // in the case that the JVM dies before the Error is passed up through the queue
                if (t instanceof Error) {
                    t.printStackTrace();
                }
                this.queue.add(new AsyncPrefetchingIterator<T>.Either(t));
            }
        }
    }

    @Override
    public void close() {
        if (this.backgroundThread == null) return;
        /*
         If prefetch thread is interrupted while awake and before acquiring permits, it will either acquire the permits
         and pass through to the next case, or check interruption status before sleeping then exit immediately
         If prefetch thread is interrupted while awake and after acquiring permits, it will check interruption status
         at the beginning of the next loop, the queue is unbounded so adding will never block
         If prefetch thread is interrupted while asleep waiting for bases, it will catch InterruptedException and exit

         Prefetch thread cannot be interrupted while awake and acquiring permits, missing the interrupt,
         because the interrupt occurs in a block synchronized on the same monitor as the acquire loop,
         so the prefetch thread must be asleep for the closing thread to acquire the lock and issue the interrupt
         */
        synchronized (this.guard) {
            this.backgroundThread.interrupt();
        }
        try {
            this.backgroundThread.join();
        } catch (final InterruptedException ie) {
            throw new RuntimeException("Interrupted waiting for background thread to complete", ie);
        } finally {
            this.inner.close();
            this.backgroundThread = null;
        }
    }

    @Override
    public boolean hasNext() {
        if (this.backgroundThread == null) {
            throw new IllegalStateException("iterator has been closed");
        }
        // Synchronized to prevent race condition where last item is taken off inner iterator
        // then there is a context switch before the item is placed onto the queue
        synchronized (this) {
            return this.inner.hasNext() || !this.queue.isEmpty();
        }
    }

    @Override
    public T next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException("SAMRecordPrefetchingIterator is empty");
        }

        final AsyncPrefetchingIterator<T>.Either next;
        try {
            next = this.queue.take();
        } catch (final InterruptedException e) {
            throw new RuntimeException("Interrupted waiting for prefetching thread", e);
        }

        if (next.item != null) {
            synchronized (this.guard) {
                this.guard.release(next.item);
                this.guard.notify();
            }
            return next.item;
        }

        // Throw any errors that were raised on the prefetch thread
        final Throwable t = next.error;
        if (t instanceof Error) {
            throw (Error) t;
        } else if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        } else {
            throw new RuntimeException(t);
        }
    }
}
