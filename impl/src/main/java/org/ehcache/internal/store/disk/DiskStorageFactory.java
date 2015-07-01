/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ehcache.internal.store.disk;

import org.ehcache.function.Predicate;
import org.ehcache.function.Predicates;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.store.disk.ods.FileAllocationTree;
import org.ehcache.internal.store.disk.ods.Region;
import org.ehcache.internal.store.disk.utils.ConcurrencyUtil;
import org.ehcache.spi.cache.AbstractValueHolder;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.FileBasedPersistenceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
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

  public interface Element<K, V> extends Serializable {
    boolean isExpired(long time);
    K getKey();
    DiskValueHolder<V> getValueHolder();
    void setFaulted(boolean faulted);
    boolean isFaulted();
  }

  static class ElementImpl<K, V> implements Element<K, V> {
    private static final long serialVersionUID = -7234449795271393813L;

    private final DiskValueHolder<V> valueHolder;
    private final K key;
    private transient boolean faulted;

    public ElementImpl(long id, K key, V value, long createTime, long expireTime) {
      this.key = key;
      this.valueHolder = new DiskValueHolder<V>(id, value, createTime, expireTime);
    }

    @Override
    public boolean isExpired(long time) {
      return !faulted && valueHolder.isExpired(time, TimeUnit.MILLISECONDS);
    }

    @Override
    public K getKey() {
      return key;
    }

    @Override
    public DiskValueHolder<V> getValueHolder() {
      return valueHolder;
    }

    @Override
    public void setFaulted(boolean faulted) {
      this.faulted = faulted;
    }

    @Override
    public boolean isFaulted() {
      return faulted;
    }
  }

  static class DiskValueHolder<V> extends AbstractValueHolder<V> {
    private static final long serialVersionUID = -7234449795271393813L;

    static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;

    private final V value;

    public DiskValueHolder(long id, V value, long createTime, long expireTime) {
      super(id, createTime, expireTime);
      this.value = value;
    }

    @Override
    public V value() {
      return value;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (other == null || getClass() != other.getClass()) return false;

      DiskValueHolder that = (DiskValueHolder)other;

      if (!super.equals(that)) return false;
      if (!value.equals(that.value)) return false;

      return true;
    }

    @Override
    final protected TimeUnit nativeTimeUnit() {
      return TIME_UNIT;
    }

    @Override
    public int hashCode() {
      int result = 1;
      result = 31 * result + value.hashCode();
      result = 31 * result + super.hashCode();
      return result;
    }
  }


  /**
   * Path stub used to create unique ehcache directories.
   */
  private static final int SHUTDOWN_GRACE_PERIOD = 60;
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

  private final TimeSource timeSource;

  private final Serializer<Element> elementSerializer;
  private final Serializer<Object> indexSerializer;
  private final long capacity;
  private final Predicate<DiskStorageFactory.DiskSubstitute<K, V>> evictionVeto;
  private final Comparator<DiskSubstitute<K, V>> evictionPrioritizer;

  /**
   * Constructs an disk persistent factory for the given cache and disk path.
   */
  public DiskStorageFactory(long capacity, Predicate<DiskStorageFactory.DiskSubstitute<K, V>> evictionVeto,
                            Comparator<DiskSubstitute<K, V>> evictionPrioritizer, TimeSource timeSource,
                            Serializer<Element> elementSerializer, Serializer<Object> indexSerializer, FileBasedPersistenceContext persistenceContext,
                            int stripes, long queueCapacity, int expiryThreadInterval) throws FileNotFoundException {
    this.capacity = capacity;
    this.evictionVeto = evictionVeto;
    this.evictionPrioritizer = evictionPrioritizer;
    this.timeSource = timeSource;
    this.elementSerializer = elementSerializer;
    this.indexSerializer = indexSerializer;
    this.file = persistenceContext.getDataFile();
    this.indexFile = persistenceContext.getIndexFile();

    if (!file.exists() || !indexFile.exists()) {
      throw new FileNotFoundException("Data file " + file + " or index file " + indexFile + " missing.");
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

    diskWriter.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    diskWriter.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
    diskWriter.scheduleWithFixedDelay(new DiskExpiryTask(), (long) expiryThreadInterval, (long) expiryThreadInterval, TimeUnit.SECONDS);

    flushTask = new IndexWriteTask(indexFile);
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
   * @param lock       the lock protecting the DiskSubstitute
   * @param substitute DiskSubstitute being freed.
   */
  public void free(Lock lock, DiskSubstitute<K, V> substitute) {
    free(lock, substitute, false);
  }

  /**
   * Free any manually managed resources used by this {@link DiskSubstitute}.
   *
   * @param lock         the lock protecting the DiskSubstitute
   * @param substitute   DiskSubstitute being freed.
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
   * <p/>
   * This shuts down the executor and then waits for its termination, before closing the data file.
   *
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
  }

  /**
   * Schedule to given task on the disk writer executor service.
   *
   * @param <U>  return type of the callable
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
   * @throws java.io.IOException    on read error
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

    return elementSerializer.read(ByteBuffer.wrap(buffer));
  }


  /**
   * Write the given element to disk, and return the associated marker.
   *
   * @param element to write
   * @return marker representing the element
   * @throws java.io.IOException on write error
   */
  protected DiskMarker<K, V> write(Element<K, V> element) throws IOException {
    ByteBuffer buffer = serializeElement(element);
    int bufferLength = buffer.remaining();
    elementSize = bufferLength;
    DiskMarker<K, V> marker = alloc(element, bufferLength);
    // Write the record
    final RandomAccessFile data = getDataAccess(element.getKey());
    synchronized (data) {
      data.seek(marker.getPosition());
      byte[] bytes = new byte[bufferLength];
      buffer.get(bytes);
      data.write(bytes, 0, bufferLength);
    }
    return marker;
  }

  private ByteBuffer serializeElement(Element<K, V> element) throws IOException {
    // A ConcurrentModificationException can occur because Java's serialization
    // mechanism is not threadsafe and POJOs are seldom implemented in a threadsafe way.
    // e.g. we are serializing an ArrayList field while another thread somewhere in the application is appending to it.
    try {
      return elementSerializer.serialize(element);
    } catch (ConcurrentModificationException e) {
      throw new RuntimeException("Failed to serialize element due to ConcurrentModificationException. " +
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
     * <p/>
     * Used during deserialization of markers to associate them with the deserializing factory.
     *
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
     *
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
      return getElement().getValueHolder().hitRate(TimeUnit.SECONDS);
    }

    @Override
    long getExpirationTime() {
      return getElement().getValueHolder().expirationTime(DiskValueHolder.TIME_UNIT);
    }

    /**
     * Return the element that this Placeholder is wrapping.
     *
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
     * @param factory  factory responsible for this marker
     * @param position position on disk where the element will be stored
     * @param size     size of the serialized element
     * @param element  element being stored
     */
    DiskMarker(DiskStorageFactory<K, V> factory, long position, int size, Element<K, V> element) {
      super(factory);
      this.position = position;
      this.size = size;

      this.key = element.getKey();
      this.hitRate = element.getValueHolder().hitRate(TimeUnit.SECONDS);
      this.expiry = element.getValueHolder().expirationTime(DiskValueHolder.TIME_UNIT);
    }

    /**
     * Create a new marker tied to the given factory instance.
     *
     * @param factory  factory responsible for this marker
     * @param position position on disk where the element will be stored
     * @param size     size of the serialized element
     * @param key      key to which this element is mapped
     * @param hits     hit count for this element
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
     * <p/>
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
      expiry = e.getValueHolder().expirationTime(DiskValueHolder.TIME_UNIT);
    }

    /**
     * Updates the stats from memory
     */
    void updateStats(float hitRate, long expireTimeMillis) {
      this.hitRate = hitRate;
      this.expiry = expireTimeMillis;
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
      Iterator<DiskSubstitute<K, V>> diskSubstituteIterator = store.diskSubstituteIterator();
      while (diskSubstituteIterator.hasNext()) {
        DiskSubstitute<K, V> value = diskSubstituteIterator.next();
        if (created(value) && value instanceof DiskStorageFactory.DiskMarker) {
          checkExpiry((DiskMarker) value, now);
        }
      }
    }

    private void checkExpiry(DiskMarker<K, V> marker, long now) {
      if (marker.getExpirationTime() < now) {
        store.expire(marker.getKey(), marker);
      }
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
        Element<K, V> read = read(marker);
        //TODO update with the true hit rate once it has been implemented, see #122
        read.getValueHolder().setExpirationTime(((DiskMarker) object).expiry, DiskValueHolder.TIME_UNIT);
        return read;
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
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
        throw new RuntimeException(e);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
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
  public void unbind() {
    try {
      flush().get();
    } catch (Throwable t) {
      LOG.error("Could not flush disk cache. Initial cause was " + t.getMessage(), t);
    }

    try {
      shutdown();
    } catch (IOException e) {
      LOG.error("Could not shut down disk cache. Initial cause was " + e.getMessage(), e);
    }
  }

  /**
   * Schedule a flush (index write) for this factory.
   *
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
      DiskSubstitute<K, V> target = this.getDiskEvictionTarget(null, count, evictionVeto);
      if (target == null) {
        // 2nd attempt without any veto
        target = this.getDiskEvictionTarget(null, count, Predicates.<DiskSubstitute<K, V>>none());
      }

      if (target != null) {
        Element<K, V> evictedElement = store.evict(target.getKey(), null);
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

  void evictToSize() {
    onDiskEvict(onDisk.get(), null);
  }

  private void onDiskEvict(int size, K keyHint) {
    long diskCapacity = capacity;

    if (diskCapacity > 0) {
      long delta = size - diskCapacity;
      int overflow = delta <= 0 ? 0 : (int) delta;
      for (int i = 0; i < Math.min(MAX_EVICT, overflow); i++) {
        DiskSubstitute<K, V> target = getDiskEvictionTarget(keyHint, size, evictionVeto);
        if (target == null) {
          // 2nd attempt without any veto
          target = this.getDiskEvictionTarget(keyHint, size, Predicates.<DiskSubstitute<K, V>>none());
        }
        if (target != null) {
          final Element<K, V> element = store.evict(target.getKey(), target);
          if (element != null && onDisk.get() <= diskCapacity) {
            break;
          }
        }
      }
    }
  }

  private DiskSubstitute<K, V> getDiskEvictionTarget(K keyHint, int size, Predicate<DiskSubstitute<K, V>> evictionVeto) {
    List<DiskSubstitute<K, V>> sample = store.getRandomSample(onDiskFilter, Math.min(SAMPLE_SIZE, size), keyHint);
    DiskSubstitute<K, V> target = null;
    DiskSubstitute<K, V> hintTarget = null;

    Collections.sort(sample, evictionPrioritizer);
    for (DiskSubstitute<K, V> substitute : sample) {
      if (evictionVeto.test(substitute)) {
        continue;
      }
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

    /**
     * Create a disk flush task that writes to the given file.
     *
     * @param index the file to write the index to
     */
    IndexWriteTask(File index) {
      this.index = index;
    }

    /**
     * {@inheritDoc}
     */
    public synchronized Void call() throws IOException, InterruptedException {
      WritableByteChannel writableByteChannel = Channels.newChannel(new FileOutputStream(index));
      try {
        Iterator<DiskSubstitute<K, V>> diskSubstituteIterator = store.diskSubstituteIterator();
        while (diskSubstituteIterator.hasNext()) {
          DiskSubstitute<K, V> o = diskSubstituteIterator.next();
          K key = o.getKey();
          if (o instanceof Placeholder && !((Placeholder) o).failedToFlush) {
            o = new PersistentDiskWriteTask<K, V>((Placeholder) o, DiskStorageFactory.this).call();
            if (o == null) {
              o = store.unretrievedGet(key);
            }
          }

          if (o instanceof DiskMarker) {
            DiskMarker<K, V> marker = (DiskMarker) o;

            writeMarker(writableByteChannel, marker);
          }
        }
      } finally {
        writableByteChannel.close();
      }
      return null;
    }

    private void writeMarker(WritableByteChannel writableByteChannel, DiskMarker<K, V> marker) throws IOException {
      ByteBuffer markerBuffer = indexSerializer.serialize(marker);
      ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
      sizeBuffer.clear();
      sizeBuffer.putInt(markerBuffer.limit());
      sizeBuffer.rewind();
      writableByteChannel.write(sizeBuffer);
      writableByteChannel.write(markerBuffer);
    }

  }

  private void loadIndex() {
    if (!indexFile.exists()) {
      return;
    }

    try {
      ReadableByteChannel readableByteChannel = Channels.newChannel(new FileInputStream(indexFile));
      try {
        DiskMarker<K, V> marker = readMarker(readableByteChannel);

        while (true) {
          marker.bindFactory(this);
          markUsed(marker);
          if (store.putRawIfAbsent(marker.getKey(), marker)) {
            onDisk.incrementAndGet();
          } else {
            // the disk pool is full
            return;
          }
          marker = readMarker(readableByteChannel);
        }
      } finally {
        readableByteChannel.close();
      }
    } catch (EOFException e) {
      // end of file reached, stop processing
    } catch (Exception e) {
      LOG.warn("Index file {} is corrupt, deleting and ignoring it : {}", indexFile, e);
      LOG.debug("Corrupt index file {} error :", indexFile, e);
      store.internalClear();
    } finally {
      shrinkDataFile();
    }
  }

  private DiskMarker<K, V> readMarker(ReadableByteChannel readableByteChannel) throws IOException, ClassNotFoundException {
    ByteBuffer sizeBuffer = ByteBuffer.allocate(4);

    sizeBuffer.clear();
    int read = readableByteChannel.read(sizeBuffer);
    if (read == -1) {
      throw new EOFException();
    }
    sizeBuffer.rewind();
    ByteBuffer valueBuffer = ByteBuffer.allocate(sizeBuffer.getInt());
    read = readableByteChannel.read(valueBuffer);
    if (read == -1) {
      throw new EOFException();
    }
    valueBuffer.rewind();
    return (DiskMarker) indexSerializer.read(valueBuffer);
  }

}
