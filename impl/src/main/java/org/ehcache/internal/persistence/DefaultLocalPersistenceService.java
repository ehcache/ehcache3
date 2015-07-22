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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.lang.Integer.toHexString;
import static java.nio.charset.Charset.forName;

/**
 * @author Alex Snaps
 */
public class DefaultLocalPersistenceService implements LocalPersistenceService {

  private static final Charset UTF8 = forName("UTF8");
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

  private final Map<String, FileBasedPersistenceContext> knownPersistenceContexts = new ConcurrentHashMap<String, FileBasedPersistenceContext>();
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
  public synchronized void start(final ServiceProvider serviceProvider) {
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
    String stringIdentifier = validateIdentifier(identifier);
    DefaultFileBasedPersistenceContext context = new DefaultFileBasedPersistenceContext(getDirectoryFor(stringIdentifier));
    knownPersistenceContexts.put(stringIdentifier, context);

    if (!storeConfiguration.isPersistent()) {
      destroy(stringIdentifier, context, false);
    }

    try {
      create(context);
    } catch (IOException e) {
      knownPersistenceContexts.remove(stringIdentifier);
      throw new CachePersistenceException("Unable to create persistence context for " + identifier, e);
    }

    return context;
  }

  @Override
  public void destroyPersistenceContext(Object identifier) {
    String stringIdentifier = validateIdentifier(identifier);
    FileBasedPersistenceContext persistenceContext = knownPersistenceContexts.get(stringIdentifier);
    if (persistenceContext != null) {
      try {
        destroy(stringIdentifier, persistenceContext, true);
      } finally {
        knownPersistenceContexts.remove(stringIdentifier);
      }
    } else {
      destroy(stringIdentifier, new DefaultFileBasedPersistenceContext(getDirectoryFor(stringIdentifier)), true);
    }
  }

  File getLockFile() {
    return lockFile;
  }
  
  public File getDirectoryFor(String identifier) {
    File directory = new File(rootDirectory, safeIdentifier(identifier));
    
    for (File parent = directory.getParentFile(); parent != null; parent = parent.getParentFile()) {
      if (rootDirectory.equals(parent)) {
        return directory;
      }
    }

    throw new IllegalArgumentException("Attempted to access file outside the persistence path");
  }
  
  private static String validateIdentifier(Object identifier) {
    if (identifier instanceof String) {
      return (String) identifier;
    } else {
      throw new IllegalArgumentException("Currently only String identifiers are supported");
    }
  }

  static void create(FileBasedPersistenceContext fileBasedPersistenceContext) throws IOException, CachePersistenceException {
    File persistenceDirectory = fileBasedPersistenceContext.getDirectory();
    
    if (persistenceDirectory.isDirectory()) {
      LOGGER.info("Reusing " + persistenceDirectory.getAbsolutePath());
    } else if (persistenceDirectory.mkdir()) {
      LOGGER.info("Created " + persistenceDirectory.getAbsolutePath());
    } else {
      throw new CachePersistenceException("Unable to create or reuse persistence context state: " + persistenceDirectory.getAbsolutePath());
    }
  }

  static void destroy(String identifier, FileBasedPersistenceContext fileBasedPersistenceContext, boolean verbose) {
    if (verbose) {
      LOGGER.info("Destroying file based persistence context for {}", identifier);
    }
    if (fileBasedPersistenceContext.getDirectory().exists() && !recursiveDelete(fileBasedPersistenceContext.getDirectory())) {
      if (verbose) {
        LOGGER.warn("Could not delete directory for context {}", identifier);
      }
    }
  }

  private static boolean recursiveDelete(File file) {
    Deque<File> toDelete = new ArrayDeque<File>();
    toDelete.push(file);
    while (!toDelete.isEmpty()) {
      File target = toDelete.pop();
      if (target.isFile()) {
        if (!target.delete()) {
          return false;
        }
      } else if (target.isDirectory()) {
        File[] contents = target.listFiles();
        if (contents.length == 0) {
          if (!target.delete()) {
            return false;
          }
        } else {
          toDelete.push(target);
          for (File f : contents) {
            toDelete.push(f);
          }
        }
      }
    }
    return true;
  }

  /**
   * sanitize a name for valid file or directory name
   *
   * @param name
   * @return sanitized version of name
   */
  private static String safeIdentifier(String name) {
    int len = name.length();
    StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; i++) {
      char c = name.charAt(i);
      if (c <= ' ' || c >= DEL || ILLEGALS.contains(c) || c == ESCAPE) {
        sb.append(ESCAPE);
        sb.append(String.format("%04x", (int) c));
      } else {
        sb.append(c);
      }
    }
    sb.append("_").append(sha1(name));
    return sb.toString();
  }

  private static String sha1(String input) {
    StringBuilder sb = new StringBuilder();
    for (byte b : getSha1Digest().digest(input.getBytes(UTF8))) {
      sb.append(toHexString((b & 0xf0) >>> 4));
      sb.append(toHexString((b & 0xf)));
    }
    return sb.toString();
  }
  
  private static MessageDigest getSha1Digest() {
    try {
      return MessageDigest.getInstance("SHA-1");
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError("All JDKs must have SHA-1");
    }
  }
  
  private static class DefaultFileBasedPersistenceContext implements FileBasedPersistenceContext {

    final File directory;

    DefaultFileBasedPersistenceContext(File directory) {
      this.directory = directory;
    }

    @Override
    public File getDirectory() {
      return directory;
    }
  }
}
