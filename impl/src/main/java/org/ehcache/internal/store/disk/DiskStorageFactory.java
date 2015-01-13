/**
 *  Copyright Terracotta, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.ehcache.internal.store.disk;

import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.store.disk.ods.FileAllocationTree;
import org.ehcache.internal.store.disk.ods.Region;
import org.ehcache.internal.store.disk.utils.CacheException;
import org.ehcache.internal.store.disk.utils.ConcurrencyUtil;
import org.ehcache.internal.store.disk.utils.MemoryEfficientByteArrayOutputStream;
import org.ehcache.internal.store.disk.utils.PreferredLoaderObjectInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

/**
 * A mock-up of a on-disk element proxy factory.
 *
 * @author Chris Dennis
 * @author Ludovic Orban
 */
public class DiskStorageFactory<K, V> {

    interface Element<K, V> extends Serializable, Map.Entry<K, DiskValueHolder<V>> {
        boolean isExpired(TimeSource timeSource);
    }

    static class ElementImpl<K, V> implements Element<K, V> {
        private volatile DiskValueHolder<V> valueHolder;
        private final K key;

        public ElementImpl(K key, V value) {
            this.key = key;
            this.valueHolder = new DiskValueHolderImpl<V>(value);
        }

        @Override
        public boolean isExpired(TimeSource timeSource) {
            return valueHolder.isExpired(timeSource.getTimeMillis());
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public DiskValueHolder<V> getValue() {
            return valueHolder;
        }

        @Override
        public DiskValueHolder<V> setValue(DiskValueHolder<V> value) {
            DiskValueHolder<V> old = valueHolder;
            valueHolder = value;
            return old;
        }
    }

    static class DiskValueHolderImpl<V> implements DiskValueHolder<V>, Serializable {
      private final V value;

      public DiskValueHolderImpl(V value) {
        this.value = value;
      }

      @Override
      public void setAccessTimeMillis(long accessTime) {

      }

      @Override
      public void setExpireTimeMillis(long expireTime) {

      }

      @Override
      public boolean isExpired(long now) {
        return false;
      }

      @Override
      public long getExpireTimeMillis() {
        return 0;
      }

      @Override
      public V value() {
        return value;
      }

      @Override
      public long creationTime(TimeUnit unit) {
        return 0;
      }

      @Override
      public long lastAccessTime(TimeUnit unit) {
        return 0;
      }

      @Override
      public float hitRate(TimeUnit unit) {
        return 0;
      }
    }


    /**
     * Path stub used to create unique ehcache directories.
     */
    private static final int SERIALIZATION_CONCURRENCY_DELAY = 250;
    private static final int SHUTDOWN_GRACE_PERIOD = 60;
    private static final int MEGABYTE = 1024 * 1024;
    private static final int MAX_EVICT = 5;
    private static final int SAMPLE_SIZE = 30;

    private static final Logger LOG = LoggerFactory.getLogger(DiskStorageFactory.class.getName());

    /**
     * The store bound to this factory.
     */
    protected volatile DiskStore<K, V> store;

    private final BlockingQueue<Runnable> diskQueue;
    /**
     * Executor service used to write elements to disk
     */
    private final ScheduledThreadPoolExecutor diskWriter;

    private final long queueCapacity;

    private final File file;
    private final RandomAccessFile[] dataAccess;

    private final FileAllocationTree allocator;

    private volatile int elementSize;

    private final ElementSubstituteFilter onDiskFilter = new OnDiskFilter();

    private final AtomicInteger onDisk = new AtomicInteger();

    private final File indexFile;

    private final IndexWriteTask flushTask;

    private volatile int diskCapacity;

    private final boolean diskPersistent;

    private final TimeSource timeSource;
    private final DiskStorePathManager diskStorePathManager;
    private final String alias;

    private final ClassLoader classLoader;

    /**
     * Constructs an disk persistent factory for the given cache and disk path.
     */
    public DiskStorageFactory(ClassLoader classLoader, TimeSource timeSource, DiskStorePathManager diskStorePathManager, String alias, boolean persistent, int stripes, long queueCapacity, int maxOnDisk, int expiryThreadInterval, boolean clearOnFlush) {
        this.classLoader = classLoader;
        this.timeSource = timeSource;
        this.diskStorePathManager = diskStorePathManager;
        this.alias = alias;
        this.file = diskStorePathManager.getFile(alias, ".data");

        this.indexFile = diskStorePathManager.getFile(alias, ".index");
        this.diskPersistent = persistent;

        LOG.info(" data file : {}", file.getAbsolutePath());
        LOG.info("index file : {}", indexFile.getAbsolutePath());

        if (diskPersistent && diskStorePathManager.isAutoCreated()) {
            LOG.warn("Data in persistent disk stores is ignored for stores from automatically created directories.\n"
                    + "Remove diskPersistent or resolve the conflicting disk paths in cache configuration.\n"
                    + "Deleting data file " + file.getAbsolutePath());
            deleteFile(file);
        } else if (!diskPersistent) {
            deleteFile(file);
            deleteFile(indexFile);
        }

        try {
            dataAccess = allocateRandomAccessFiles(file, stripes);
        } catch (FileNotFoundException fnfe) {
            throw new IllegalArgumentException(fnfe);
        }
        this.allocator = new FileAllocationTree(Long.MAX_VALUE, dataAccess[0]);

        diskWriter = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, file.getName());
                t.setDaemon(false);
                return t;
            }
        });
        this.diskQueue = diskWriter.getQueue();
        this.queueCapacity = queueCapacity;
        this.diskCapacity = maxOnDisk;

        diskWriter.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        diskWriter.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        diskWriter.scheduleWithFixedDelay(new DiskExpiryTask(), (long) expiryThreadInterval, (long) expiryThreadInterval, TimeUnit.SECONDS);

        flushTask = new IndexWriteTask(indexFile, clearOnFlush);

        if (!getDataFile().exists() || (getDataFile().length() == 0)) {
            LOG.debug("Matching data file missing (or empty) for index file. Deleting index file " + indexFile);
            deleteFile(indexFile);
        } else if (getDataFile().exists() && indexFile.exists()) {
            if (getDataFile().lastModified() > (indexFile.lastModified() + TimeUnit.SECONDS.toMillis(1))) {
                LOG.warn("The index for data file {} is out of date, probably due to an unclean shutdown. "
                        + "Deleting index file {}", getDataFile(), indexFile);
                deleteFile(indexFile);
            }
        }
    }

    private static RandomAccessFile[] allocateRandomAccessFiles(File f, int stripes) throws FileNotFoundException {
        int roundedStripes = stripes;
        while ((roundedStripes & (roundedStripes - 1)) != 0) {
            ++roundedStripes;
        }

        RandomAccessFile[] result = new RandomAccessFile[roundedStripes];
        for (int i = 0; i < result.length; ++i) {
            result[i] = new RandomAccessFile(f, "rw");
        }

        return result;
    }

    private RandomAccessFile getDataAccess(Object key) {
        return this.dataAccess[ConcurrencyUtil.selectLock(key, dataAccess.length)];
    }


    /**
     * Return this size in bytes of this factory
     *
     * @return this size in bytes of this factory
     */
    public long getOnDiskSizeInBytes() {
        synchronized (dataAccess[0]) {
            try {
                return dataAccess[0].length();
            } catch (IOException e) {
                LOG.warn("Exception trying to determine store size", e);
                return 0;
            }
        }
    }

    /**
     * Bind a store instance to this factory.
     *
     * @param store store to bind
     */
    public void bind(DiskStore<K, V> store) {
        this.store = store;
        loadIndex();
    }

    /**
     * Free any manually managed resources used by this {@link DiskSubstitute}.
     *
     * @param lock the lock protecting the DiskSubstitute
     * @param substitute DiskSubstitute being freed.
     */
    public void free(Lock lock, DiskSubstitute<K, V> substitute) {
        free(lock, substitute, false);
    }

    /**
     * Free any manually managed resources used by this {@link DiskSubstitute}.
     *
     * @param lock the lock protecting the DiskSubstitute
     * @param substitute DiskSubstitute being freed.
     * @param faultFailure true if this DiskSubstitute should be freed because of a disk failure
     */
    public void free(Lock lock, DiskSubstitute<K, V> substitute, boolean faultFailure) {
        if (substitute instanceof DiskStorageFactory.DiskMarker) {
            if (!faultFailure) {
                onDisk.decrementAndGet();
            }
            //free done asynchronously under the relevant segment lock...
            DiskFreeTask free = new DiskFreeTask(lock, (DiskMarker) substitute);
            if (lock.tryLock()) {
                try {
                    free.call();
                } finally {
                    lock.unlock();
                }
            } else {
                schedule(free);
            }
        }
    }

    /**
     * Mark this on-disk marker as used (hooks into the file space allocation structure).
     *
     * @param marker on-disk marker to mark as used
     */
    protected void markUsed(DiskMarker<K, V> marker) {
        allocator.mark(new Region(marker.getPosition(), marker.getPosition() + marker.getSize() - 1));
    }

    /**
     * Shrink this store's data file down to a minimal size for its contents.
     */
    protected void shrinkDataFile() {
        synchronized (dataAccess[0]) {
            try {
                dataAccess[0].setLength(allocator.getFileSize());
            } catch (IOException e) {
                LOG.error("Exception trying to shrink data file to size", e);
            }
        }
    }
    /**
     * Shuts down this disk factory.
     * <p>
     * This shuts down the executor and then waits for its termination, before closing the data file.
     * @throws java.io.IOException if an IO error occurred
     */
    protected void shutdown() throws IOException {
        diskWriter.shutdown();
        for (int i = 0; i < SHUTDOWN_GRACE_PERIOD; i++) {
            try {
                if (diskWriter.awaitTermination(1, TimeUnit.SECONDS)) {
                    break;
                } else {
                    LOG.info("Waited " + (i + 1) + " seconds for shutdown of [" + file.getName() + "]");
                }
            } catch (InterruptedException e) {
                LOG.warn("Received exception while waiting for shutdown", e);
            }
        }

        for (final RandomAccessFile raf : dataAccess) {
            synchronized (raf) {
                raf.close();
            }
        }

        if (!diskPersistent) {
            deleteFile(file);
            deleteFile(indexFile);
        }
    }

    /**
     * Deletes the data file for this factory.
     */
    protected void delete() {
        deleteFile(file);
        allocator.clear();
    }

    /**
     * Schedule to given task on the disk writer executor service.
     *
     * @param <U> return type of the callable
     * @param call callable to call
     * @return Future representing the return of this call
     */
    protected <U> Future<U> schedule(Callable<U> call) {
        return diskWriter.submit(call);
    }

    /**
     * Read the data at the given marker, and return the associated deserialized Element.
     *
     * @param marker marker to read
     * @return deserialized Element
     * @throws java.io.IOException on read error
     * @throws ClassNotFoundException on deserialization error
     */
    protected Element<K, V> read(DiskMarker<K, V> marker) throws IOException, ClassNotFoundException {
        final byte[] buffer = new byte[marker.getSize()];
        final RandomAccessFile data = getDataAccess(marker.getKey());
        synchronized (data) {
            // Load the element
            data.seek(marker.getPosition());
            data.readFully(buffer);
        }

        ObjectInputStream objstr = new PreferredLoaderObjectInputStream(new ByteArrayInputStream(buffer), classLoader);

        try {
            return (Element) objstr.readObject();
        } finally {
            objstr.close();
        }
    }


    /**
     * Write the given element to disk, and return the associated marker.
     *
     * @param element to write
     * @return marker representing the element
     * @throws java.io.IOException on write error
     */
    protected DiskMarker<K, V> write(Element<K, V> element) throws IOException {
        MemoryEfficientByteArrayOutputStream buffer = serializeElement(element);
        int bufferLength = buffer.size();
        elementSize = bufferLength;
        DiskMarker<K, V> marker = alloc(element, bufferLength);
        // Write the record
        final RandomAccessFile data = getDataAccess(element.getKey());
        synchronized (data) {
            data.seek(marker.getPosition());
            data.write(buffer.toByteArray(), 0, bufferLength);
        }
        return marker;
    }

    private MemoryEfficientByteArrayOutputStream serializeElement(Element<K, V> element) throws IOException {
        // A ConcurrentModificationException can occur because Java's serialization
        // mechanism is not threadsafe and POJOs are seldom implemented in a threadsafe way.
        // e.g. we are serializing an ArrayList field while another thread somewhere in the application is appending to it.
        try {
            return MemoryEfficientByteArrayOutputStream.serialize(element);
        } catch (ConcurrentModificationException e) {
            throw new CacheException("Failed to serialize element due to ConcurrentModificationException. " +
                                     "This is frequently the result of inappropriately sharing thread unsafe object " +
                                     "(eg. ArrayList, HashMap, etc) between threads", e);
        }
    }

    private DiskMarker<K, V> alloc(Element<K, V> element, int size) throws IOException {
        //check for a matching chunk
        Region r = allocator.alloc(size);
        return createMarker(r.start(), size, element);
    }

    /**
     * Free the given marker to be used by a subsequent write.
     *
     * @param marker marker to be free'd
     */
    protected void free(DiskMarker<K, V> marker) {
        allocator.free(new Region(marker.getPosition(), marker.getPosition() + marker.getSize() - 1));
    }

    /**
     * Return {@code true} if the disk write queue is full.
     *
     * @return {@code true} if the disk write queue is full.
     */
    public boolean bufferFull() {
        return (diskQueue.size() * elementSize) > queueCapacity;
    }

    /**
     * Return a reference to the data file backing this factory.
     *
     * @return a reference to the data file backing this factory.
     */
    public File getDataFile() {
        return file;
    }

    /**
     * DiskWriteTasks are used to serialize elements
     * to disk and fault in the resultant DiskMarker
     * instance.
     */
    static abstract class DiskWriteTask<K, V> implements Callable<DiskMarker<K, V>> {

        private final Placeholder<K, V> placeholder;
        private final DiskStorageFactory<K, V> storageFactory;

        /**
         * Create a disk-write task for the given placeholder.
         *
         * @param p a disk-write task for the given placeholder.
         */
        DiskWriteTask(Placeholder<K, V> p, DiskStorageFactory<K, V> storageFactory) {
            this.placeholder = p;
            this.storageFactory = storageFactory;
        }

        /**
         * Return the placeholder that this task will write.
         *
         * @return the placeholder that this task will write.
         */
        Placeholder<K, V> getPlaceholder() {
            return placeholder;
        }

        /**
         * {@inheritDoc}
         */
        public DiskMarker<K, V> call() {
            try {
                if (storageFactory.store.containsKey(placeholder.getKey())) {
                    DiskMarker<K, V> marker = storageFactory.write(placeholder.getElement());
                    if (marker != null && storageFactory.store.fault(placeholder.getKey(), placeholder, marker)) {
                        return marker;
                    } else {
                        return null;
                    }
                } else {
                    return null;
                }
            } catch (Throwable e) {
                // TODO Need to clean this up once FrontEndCacheTier is going away completely
                LOG.error("Disk Write of " + placeholder.getKey() + " failed: ", e);
                storageFactory.store.evict(placeholder.getKey(), placeholder);
                return null;
            }
        }
    }

    /**
     * Disk free tasks are used to asynchronously free DiskMarker instances under the correct
     * exclusive write lock.  This ensure markers are not free'd until no more readers can be
     * holding references to them.
     */
    private final class DiskFreeTask implements Callable<Void> {
        private final Lock lock;
        private final DiskMarker<K, V> marker;

        private DiskFreeTask(Lock lock, DiskMarker<K, V> marker) {
            this.lock = lock;
            this.marker = marker;
        }

        /**
         * {@inheritDoc}
         */
        public Void call() {
            lock.lock();
            try {
                DiskStorageFactory.this.free(marker);
            } finally {
                lock.unlock();
            }
            return null;
        }
    }

    /**
     * Abstract superclass for all disk substitutes.
     */
    public abstract static class DiskSubstitute<K, V> {

        private transient volatile DiskStorageFactory<K, V> factory;

        /**
         * Create a disk substitute bound to no factory.  This constructor is used during
         * de-serialization.
         */
        public DiskSubstitute() {
            this.factory = null;
        }

        /**
         * Create a disk substitute bound to the given factory.
         *
         * @param factory the factory to bind to.
         */
        DiskSubstitute(DiskStorageFactory<K, V> factory) {
            this.factory = factory;
        }

        /**
         * Return the key to which this marker is (or should be) mapped.
         *
         * @return the key to which this marker is (or should be) mapped.
         */
        abstract K getKey();

        /**
         * Return the total number of hits on this marker
         *
         * @return the total number of hits on this marker
         */
        abstract float getHitRate();

        /**
         * Return the time at which this marker expires.
         *
         * @return the time at which this marker expires.
         */
        abstract long getExpirationTime();

        /**
         * Mark the disk substitute as installed
         */
        abstract void installed();

        /**
         * Returns the {@link DiskStorageFactory} instance that generated this <code>DiskSubstitute</code>
         *
         * @return an <code>ElementProxyFactory</code>
         */
        public final DiskStorageFactory<K, V> getFactory() {
            return factory;
        }

        /**
         * Bind this marker to a given factory.
         * <p>
         * Used during deserialization of markers to associate them with the deserializing factory.
         * @param factory the factory to bind to
         */
        void bindFactory(DiskStorageFactory<K, V> factory) {
            this.factory = factory;
        }
    }

    /**
     * Placeholder instances are put in place to prevent
     * duplicate write requests while Elements are being
     * written to disk.
     */
    final static class Placeholder<K, V> extends DiskSubstitute<K, V> {
        private final K key;
        private final Element<K, V> element;

        private volatile boolean failedToFlush;

        /**
         * Create a Placeholder wrapping the given element and key.
         *
         * @param element the element to wrap
         */
        Placeholder(Element<K, V> element, DiskStorageFactory<K, V> factory) {
            super(factory);
            this.key = element.getKey();
            this.element = element;
        }

        /**
         * Whether flushing this to disk ever failed
         * @return true if failed, otherwise false
         */
        boolean hasFailedToFlush() {
            return failedToFlush;
        }

        private void setFailedToFlush(final boolean failedToFlush) {
            this.failedToFlush = failedToFlush;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void installed() {
            getFactory().schedule(new PersistentDiskWriteTask<K, V>(this, super.factory));
        }

        /**
         * {@inheritDoc}
         */
        @Override
        K getKey() {
            return key;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        float getHitRate() {
            return getElement().getValue().hitRate(TimeUnit.SECONDS);
        }

        @Override
        long getExpirationTime() {
            return getElement().getValue().getExpireTimeMillis();
        }

        /**
         * Return the element that this Placeholder is wrapping.
         * @return the element that this Placeholder is wrapping.
         */
        Element<K, V> getElement() {
            return element;
        }
    }

    /**
     * DiskMarker instances point to the location of their
     * associated serialized Element instance.
     */
    public static class DiskMarker<K, V> extends DiskSubstitute<K, V> implements Serializable {

        private final K key;

        private final long position;
        private final int size;

        private volatile float hitRate;

        private volatile long expiry;

        /**
         * Create a new marker tied to the given factory instance.
         *
         * @param factory factory responsible for this marker
         * @param position position on disk where the element will be stored
         * @param size size of the serialized element
         * @param element element being stored
         */
        DiskMarker(DiskStorageFactory<K, V> factory, long position, int size, Element<K, V> element) {
            super(factory);
            this.position = position;
            this.size = size;

            this.key = element.getKey();
            this.hitRate = element.getValue().hitRate(TimeUnit.SECONDS);
            this.expiry = element.getValue().getExpireTimeMillis();
        }

        /**
         * Create a new marker tied to the given factory instance.
         *
         * @param factory factory responsible for this marker
         * @param position position on disk where the element will be stored
         * @param size size of the serialized element
         * @param key key to which this element is mapped
         * @param hits hit count for this element
         */
        DiskMarker(DiskStorageFactory<K, V> factory, long position, int size, K key, long hits) {
            super(factory);
            this.position = position;
            this.size = size;

            this.key = key;
            this.hitRate = hits;
        }

        /**
         * Key to which this Element is mapped.
         *
         * @return key for this Element
         */
        @Override
        K getKey() {
            return key;
        }

        /**
         * Number of hits on this Element.
         */
        @Override
        float getHitRate() {
            return hitRate;
        }

        /**
         * Disk offset at which this element is stored.
         *
         * @return disk offset
         */
        private long getPosition() {
            return position;
        }

        /**
         * Returns the size of the currently occupying element.
         *
         * @return size of the stored element
         */
        public int getSize() {
            return size;
        }

        /**
         * {@inheritDoc}
         * <p>
         * A No-Op
         */
        @Override
        public void installed() {
            //no-op
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public long getExpirationTime() {
            return expiry;
        }

        /**
         * Increment statistic associated with a hit on this cache.
         *
         * @param e element deserialized from disk
         */
        void hit(Element<K, V> e) {
            hitRate++;
            expiry = e.getValue().getExpireTimeMillis();
        }

        /**
         * Updates the stats from memory
         * @param e
         */
        void updateStats(Element<K, V> e) {
            hitRate = e.getValue().hitRate(TimeUnit.SECONDS);
            expiry = e.getValue().getExpireTimeMillis();
        }
    }


    /**
     * Remove elements created by this factory if they have expired.
     */
    public void expireElements() {
        new DiskExpiryTask().run();
    }

    /**
     * Causes removal of all expired elements (and fires the relevant events).
     */
    private final class DiskExpiryTask implements Runnable {

        /**
         * {@inheritDoc}
         */
        public void run() {
            long now = timeSource.getTimeMillis();
            for (K key : store.keySet()) {
                DiskSubstitute<K, V> value = store.unretrievedGet(key);
                if (created(value) && value instanceof DiskStorageFactory.DiskMarker) {
                    checkExpiry((DiskMarker) value, now);
                }
            }
        }

        private void checkExpiry(DiskMarker<K, V> marker, long now) {
            if (marker.getExpirationTime() < now) {
                store.evict(marker.getKey(), marker);
            }
        }
    }

    /**
     * Attempt to delete the corresponding file and log an error on failure.
     * @param f the file to delete
     */
    protected static void deleteFile(File f) {
        if (!f.delete()) {
            LOG.debug("Failed to delete file {}", f.getName());
        }
    }


    /**
     * Create a disk substitute for an element
     *
     * @param element the element to create a disk substitute for
     * @return The substitute element
     * @throws IllegalArgumentException if element cannot be substituted
     */
    public DiskSubstitute<K, V> create(Element<K, V> element) throws IllegalArgumentException {
        return new Placeholder<K, V>(element, this);
    }

    /**
     * Decodes the supplied {@link DiskSubstitute}.
     *
     * @param object ElementSubstitute to decode
     * @return the decoded element
     */
    public Element<K, V> retrieve(DiskSubstitute<K, V> object) {
        if (object instanceof DiskMarker) {
            try {
                DiskMarker<K, V> marker = (DiskMarker) object;
                return read(marker);
            } catch (IOException e) {
                throw new CacheException(e);
            } catch (ClassNotFoundException e) {
                throw new CacheException(e);
            }
        } else if (object instanceof DiskStorageFactory.Placeholder) {
            return ((Placeholder) object).getElement();
        } else {
            return null;
        }
    }

    /**
     * Decodes the supplied {@link DiskSubstitute}, updating statistics.
     *
     * @param object ElementSubstitute to decode
     * @return the decoded element
     */
    public Element<K, V> retrieve(DiskSubstitute<K, V> object, Segment<K, V> segment) {
        if (object instanceof DiskMarker) {
            try {
                DiskMarker<K, V> marker = (DiskMarker) object;
                Element<K, V> e = read(marker);
                marker.hit(e);
                return e;
            } catch (IOException e) {
                throw new CacheException(e);
            } catch (ClassNotFoundException e) {
                throw new CacheException(e);
            }
        } else if (object instanceof DiskStorageFactory.Placeholder) {
            return ((Placeholder) object).getElement();
        } else {
            return null;
        }
    }

    /**
     * Returns <code>true</code> if this factory created the given object.
     *
     * @param object object to check
     * @return <code>true</code> if object created by this factory
     */
    public boolean created(Object object) {
        if (object instanceof DiskSubstitute) {
            return ((DiskSubstitute) object).getFactory() == this;
        } else {
            return false;
        }
    }

    /**
     * Unbinds a store instance from this factory
     */
    public void unbind(boolean destroy) {
        try {
            flushTask.call();
        } catch (Throwable t) {
            LOG.error("Could not flush disk cache. Initial cause was " + t.getMessage(), t);
        }

        try {
            shutdown();
            if (destroy || diskStorePathManager.isAutoCreated()) {
                deleteFile(indexFile);
                delete();
            }
        } catch (IOException e) {
            LOG.error("Could not shut down disk cache. Initial cause was " + e.getMessage(), e);
        }
    }

    /**
     * Schedule a flush (index write) for this factory.
     * @return a Future
     */
    public Future<Void> flush() {
        return schedule(flushTask);
    }

    private DiskMarker<K, V> createMarker(long position, int size, Element<K, V> element) {
        return new DiskMarker<K, V>(this, position, size, element);
    }

    /**
     * Evict some elements, if possible
     *
     * @param count the number of elements to evict
     * @return the number of elements actually evicted
     */
    int evict(int count) {
        // see void onDiskEvict(int size, Object keyHint)
        int evicted = 0;
        for (int i = 0; i < count; i++) {
            DiskSubstitute<K, V> target = this.getDiskEvictionTarget(null, count);
            if (target != null) {
                Element<K, V> evictedElement = store.evictElement(target.getKey(), null);
                if (evictedElement != null) {
                    evicted++;
                }
            }
        }
        return evicted;
    }

    /**
     * Filters for on-disk elements created by this factory
     */
    private class OnDiskFilter implements ElementSubstituteFilter {

        /**
         * {@inheritDoc}
         */
        public boolean allows(Object object) {
            if (!created(object)) {
                return false;
            }

            return object instanceof DiskMarker;
        }
    }

    /**
     * Return the number of on-disk elements
     *
     * @return the number of on-disk elements
     */
    public int getOnDiskSize() {
        return onDisk.get();
    }

    /**
     * Set the maximum on-disk capacity for this factory.
     *
     * @param capacity the maximum on-disk capacity for this factory.
     */
    public void setOnDiskCapacity(int capacity) {
        diskCapacity = capacity;
    }

    /**
     * accessor to the on-disk capacity
     * @return the capacity
     */
    int getDiskCapacity() {
        return diskCapacity == 0 ? Integer.MAX_VALUE : diskCapacity;
    }

    private void onDiskEvict(int size, K keyHint) {
        if (diskCapacity > 0) {
            int overflow = size - diskCapacity;
            for (int i = 0; i < Math.min(MAX_EVICT, overflow); i++) {
                DiskSubstitute<K, V> target = getDiskEvictionTarget(keyHint, size);
                if (target != null) {
                    final Element<K, V> element = store.evictElement(target.getKey(), target);
                    if (element != null && onDisk.get() <= diskCapacity) {
                        break;
                    }
                }
            }
        }
    }

    private DiskSubstitute<K, V> getDiskEvictionTarget(K keyHint, int size) {
        List<DiskSubstitute<K, V>> sample = store.getRandomSample(onDiskFilter, Math.min(SAMPLE_SIZE, size), keyHint);
        DiskSubstitute<K, V> target = null;
        DiskSubstitute<K, V> hintTarget = null;
        for (DiskSubstitute<K, V> substitute : sample) {
            if ((target == null) || (substitute.getHitRate() < target.getHitRate())) {
                if (substitute.getKey().equals(keyHint)) {
                    hintTarget = substitute;
                } else {
                    target = substitute;
                }
            }
        }
        return target != null ? target : hintTarget;
    }

    /**
     * Disk write task implementation for disk persistent stores.
     */
    private static final class PersistentDiskWriteTask<K, V> extends DiskWriteTask<K, V> {

        /**
         * Create a disk persistent disk-write task for this placeholder.
         *
         * @param p the placeholder
         */
        PersistentDiskWriteTask(Placeholder<K, V> p, DiskStorageFactory<K, V> storageFactory) {
            super(p, storageFactory);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public DiskMarker<K, V> call() {
            DiskMarker<K, V> result = super.call();
            if (result != null) {
                int disk = super.storageFactory.onDisk.incrementAndGet();
                super.storageFactory.onDiskEvict(disk, getPlaceholder().getKey());
            }
            return result;
        }
    }

    /**
     * Task that writes the index file for this factory.
     */
    class IndexWriteTask implements Callable<Void> {

        private final File index;
        private final boolean clearOnFlush;

        /**
         * Create a disk flush task that writes to the given file.
         *
         * @param index the file to write the index to
         * @param clear clear on flush flag
         */
        IndexWriteTask(File index, boolean clear) {
            this.index = index;
            this.clearOnFlush = clear;
        }

        /**
         * {@inheritDoc}
         */
        public synchronized Void call() throws IOException, InterruptedException {
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(index));
            try {
                for (K key : store.keySet()) {
                    DiskSubstitute<K, V> o = store.unretrievedGet(key);
                    if (o instanceof Placeholder && !((Placeholder)o).failedToFlush) {
                        o = new PersistentDiskWriteTask<K, V>((Placeholder) o, DiskStorageFactory.this).call();
                        if (o == null) {
                            o = store.unretrievedGet(key);
                        }
                    }

                    if (o instanceof DiskMarker) {
                        DiskMarker<K, V> marker = (DiskMarker) o;
                        oos.writeObject(key);
                        oos.writeObject(marker);
                    }
                }
            } finally {
                oos.close();
            }
            return null;
        }

    }

    private void loadIndex() {
        if (!indexFile.exists()) {
            return;
        }

        try {
            ObjectInputStream ois = new PreferredLoaderObjectInputStream(new FileInputStream(indexFile), classLoader);
            try {
                K key = (K) ois.readObject();
                Object value = ois.readObject();

                DiskMarker<K, V> marker = (DiskMarker) value;
                while (true) {
                    marker.bindFactory(this);
                    markUsed(marker);
                    if (store.putRawIfAbsent(key, marker)) {
                        onDisk.incrementAndGet();
                    } else {
                        // the disk pool is full
                        return;
                    }
                    key = (K) ois.readObject();
                    marker = (DiskMarker) ois.readObject();
                }
            } finally {
                ois.close();
            }
        } catch (EOFException e) {
            // end of file reached, stop processing
        } catch (Exception e) {
            LOG.warn("Index file {} is corrupt, deleting and ignoring it : {}", indexFile, e);
            LOG.debug("Corrupt index file {} error :", indexFile, e);
            try {
                store.clear();
            } catch (CacheAccessException cae) {
                LOG.warn("Error clearing disk store " + alias, cae);
            }
            deleteFile(indexFile);
        } finally {
            shrinkDataFile();
        }
    }

    /**
     * Return the index file for this store.
     * @return the index file
     */
    public File getIndexFile() {
        return indexFile;
    }
}
