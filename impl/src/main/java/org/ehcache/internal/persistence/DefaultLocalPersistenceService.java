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

package org.ehcache.internal.persistence;

import org.ehcache.config.persistence.PersistenceConfiguration;
import org.ehcache.internal.concurrent.ConcurrentHashMap;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.service.FileBasedPersistenceContext;
import org.ehcache.spi.service.LocalPersistenceService;
import org.ehcache.exceptions.CachePersistenceException;
import org.ehcache.spi.service.ServiceConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Alex Snaps
 */
public class DefaultLocalPersistenceService implements LocalPersistenceService {

  private static final int DEL = 0x7F;
  private static final char ESCAPE = '%';
  private static final Set<Character> ILLEGALS = new HashSet<Character>();
  static {
    ILLEGALS.add('/');
    ILLEGALS.add('\\');
    ILLEGALS.add('<');
    ILLEGALS.add('>');
    ILLEGALS.add(':');
    ILLEGALS.add('"');
    ILLEGALS.add('|');
    ILLEGALS.add('?');
    ILLEGALS.add('*');
    ILLEGALS.add('.');
  }
  
  private final Map<Object, FileBasedPersistenceContext> knownPersistenceContexts = new ConcurrentHashMap<Object, FileBasedPersistenceContext>();
  private final File rootDirectory;
  private final File lockFile;
  private FileLock lock;

  private RandomAccessFile rw;

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultLocalPersistenceService.class);

  private boolean started;

  public DefaultLocalPersistenceService(final PersistenceConfiguration persistenceConfiguration) {
    if(persistenceConfiguration != null) {
      rootDirectory = persistenceConfiguration.getRootDirectory();
    } else {
      throw new NullPointerException("PersistenceConfiguration cannot be null");
    }
    lockFile = new File(rootDirectory, ".lock");
  }

  @Override
  public synchronized void start(final ServiceConfiguration<?> config, final ServiceProvider serviceProvider) {
    if (!started) {
      createLocationIfRequiredAndVerify(rootDirectory);
      try {
        rw = new RandomAccessFile(lockFile, "rw");
        lock = rw.getChannel().lock();
      } catch (IOException e) {
        throw new RuntimeException("Couldn't lock rootDir: " + rootDirectory.getAbsolutePath(), e);
      }
      started = true;
      LOGGER.debug("RootDirectory Locked");
    }
  }

  @Override
  public synchronized void stop() {
    if (started) {
      try {
        lock.release();
        // Closing RandomAccessFile so that files gets deleted on windows and
        // org.ehcache.internal.persistence.DefaultLocalPersistenceServiceTest.testLocksDirectoryAndUnlocks()
        // passes on windows
        rw.close();
        if (!lockFile.delete()) {
          LOGGER.debug("Lock file was not deleted {}.", lockFile.getPath());
        }
      } catch (IOException e) {
        throw new RuntimeException("Couldn't unlock rootDir: " + rootDirectory.getAbsolutePath(), e);
      }
      started = false;
      LOGGER.debug("RootDirectory Unlocked");
    }
  }

  static void createLocationIfRequiredAndVerify(final File rootDirectory) {
    if(!rootDirectory.exists()) {
      if(!rootDirectory.mkdirs()) {
        throw new IllegalArgumentException("Directory couldn't be created: " + rootDirectory.getAbsolutePath());
      }
    } else if(!rootDirectory.isDirectory()) {
      throw new IllegalArgumentException("Location is not a directory: " + rootDirectory.getAbsolutePath());
    }

    if(!rootDirectory.canWrite()) {
      throw new IllegalArgumentException("Location isn't writable: " + rootDirectory.getAbsolutePath());
    }
  }

  @Override
  public FileBasedPersistenceContext createPersistenceContext(Object identifier, Store.PersistentStoreConfiguration<?, ?, ?> storeConfiguration) throws CachePersistenceException {
    DefaultFiledBasedPersistenceContext filedBased = new DefaultFiledBasedPersistenceContext(getFile(identifier, ".data"), getFile(identifier, ".index"));
    knownPersistenceContexts.put(identifier, filedBased);

    if (!storeConfiguration.isPersistent()) {
      destroy(identifier, filedBased, false);
    }

    try {
      create(filedBased);
    } catch (IOException e) {
      knownPersistenceContexts.remove(identifier);
      throw new CachePersistenceException("Unable to create persistence context for " + identifier, e);
    }

    return filedBased;
  }

  @Override
  public void destroyPersistenceContext(Object identifier) {
    FileBasedPersistenceContext persistenceContext = knownPersistenceContexts.get(identifier);
    if (persistenceContext != null) {
      try {
        destroy(identifier, persistenceContext, true);
      } finally {
        knownPersistenceContexts.remove(identifier);
      }
    } else {
      destroy(identifier, new DefaultFiledBasedPersistenceContext(getFile(identifier, ".data"), getFile(identifier, ".index")), true);
    }
  }

  File getLockFile() {
    return lockFile;
  }

  /**
   * Get a file object for the given cache-name and suffix
   *
   * @param identifier Some stable identifier
   * @param suffix    a file suffix
   * @return a file object
   */
  public File getFile(Object identifier, String suffix) {
    if(identifier instanceof String) {
      return getFile(safeName(((String) identifier)) + suffix);
    }
    throw new IllegalArgumentException("Currently only String identifiers are supported");
  }

  /**
   * Get a file object for the given name
   *
   * @param name the file name
   * @return a file object
   */
  public File getFile(String name) {
    File file = new File(rootDirectory, name);
    
    for (File parent = file.getParentFile(); parent != null; parent = parent.getParentFile()) {
      if (rootDirectory.equals(parent)) {
        return file;
      }
    }

    throw new IllegalArgumentException("Attempted to access file outside the persistence path");
  }


  static void create(FileBasedPersistenceContext fileBasedPersistenceContext) throws IOException, CachePersistenceException {
    boolean dataFileCreated = fileBasedPersistenceContext.getDataFile().createNewFile();
    boolean indexFileCreated = fileBasedPersistenceContext.getIndexFile().createNewFile();

    if (dataFileCreated && indexFileCreated) {
      LOGGER.info("Created " + fileBasedPersistenceContext.getDataFile().getAbsolutePath() + " and " + fileBasedPersistenceContext
          .getIndexFile()
          .getAbsolutePath());
    } else if (!dataFileCreated && !indexFileCreated) {
      LOGGER.info("Reusing " + fileBasedPersistenceContext.getDataFile().getAbsolutePath() + " and " + fileBasedPersistenceContext
          .getIndexFile().getAbsolutePath());
    } else {
      if (indexFileCreated) {
        if (fileBasedPersistenceContext.getDataFile().delete()) {
          if (!fileBasedPersistenceContext.getDataFile().createNewFile()) {
            throw new CachePersistenceException("Unable to create data file for persistence context");
          }
          LOGGER.warn("Index file " + fileBasedPersistenceContext.getIndexFile().getAbsolutePath() + " was missing, dropped previously persisted data");
        } else {
          throw new CachePersistenceException("Unable to clean up inconsistent persistence context state - no index file, old data file cannot be deleted");
        }
      }
      if (dataFileCreated) {
        if (fileBasedPersistenceContext.getIndexFile().delete()) {
          if (!fileBasedPersistenceContext.getIndexFile().createNewFile()) {
            throw new CachePersistenceException("Unable to create index file for persistence context");
          }
          LOGGER.warn("Data file " + fileBasedPersistenceContext.getDataFile().getAbsolutePath() + " was missing,  dropped previously persisted data");
        } else {
          throw new CachePersistenceException("Unable to clean up inconsistent persistence context state - no data file, old index file cannot be deleted");
        }
      }
    }
  }

  static void destroy(Object identifier, FileBasedPersistenceContext fileBasedPersistenceContext, boolean verbose) {
    if (verbose) {
      LOGGER.info("Destroying file based persistence context for {}", identifier);
    }
    if (fileBasedPersistenceContext.getDataFile().exists() && !fileBasedPersistenceContext.getDataFile().delete()) {
      if (verbose) {
        LOGGER.warn("Could not delete data file for context {}", identifier);
      }
    }
    if (fileBasedPersistenceContext.getIndexFile().exists() && !fileBasedPersistenceContext.getIndexFile().delete()) {
      if (verbose) {
        LOGGER.warn("Could not delete index file for context {}", identifier);
      }
    }
  }

  /**
   * sanitize a name for valid file or directory name
   *
   * @param name
   * @return sanitized version of name
   */
  private static String safeName(String name) {
    int len = name.length();
    StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; i++) {
      char c = name.charAt(i);
      if (c <= ' ' || c >= DEL || (c >= 'A' && c <= 'Z') || ILLEGALS.contains(c) || c == ESCAPE) {
        sb.append(ESCAPE);
        sb.append(String.format("%04x", (int) c));
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  private static class DefaultFiledBasedPersistenceContext implements FileBasedPersistenceContext {

    final File dataFile;
    final File indexFile;

    DefaultFiledBasedPersistenceContext(File dataFile, File indexFile) {
      this.dataFile = dataFile;
      this.indexFile = indexFile;
    }

    @Override
    public File getDataFile() {
      return dataFile;
    }

    @Override
    public File getIndexFile() {
      return indexFile;
    }
  }
}
