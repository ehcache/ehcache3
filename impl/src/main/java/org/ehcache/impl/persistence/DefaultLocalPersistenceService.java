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

package org.ehcache.impl.persistence;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;
import org.ehcache.impl.config.persistence.DefaultPersistenceConfiguration;
import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.core.spi.service.FileBasedPersistenceContext;
import org.ehcache.core.spi.service.LocalPersistenceService;
import org.ehcache.CachePersistenceException;
import org.ehcache.spi.service.MaintainableService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
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
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import static java.lang.Integer.toHexString;
import static java.nio.charset.Charset.forName;

import java.util.concurrent.ConcurrentMap;

/**
 * Default implementation of the {@link LocalPersistenceService} which can be used explicitly when
 * {@link org.ehcache.PersistentUserManagedCache persistent user managed caches} are desired.
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

  private final ConcurrentMap<String, DefaultPersistenceSpaceIdentifier> knownPersistenceSpaces = new ConcurrentHashMap<String, DefaultPersistenceSpaceIdentifier>();
  private final File rootDirectory;
  private final File lockFile;
  private FileLock lock;

  private RandomAccessFile rw;

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultLocalPersistenceService.class);

  private boolean started;

  /**
   * Creates a new service instance using the provided configuration.
   *
   * @param persistenceConfiguration the configuration to use
   */
  public DefaultLocalPersistenceService(final DefaultPersistenceConfiguration persistenceConfiguration) {
    if(persistenceConfiguration != null) {
      rootDirectory = persistenceConfiguration.getRootDirectory();
    } else {
      throw new NullPointerException("DefaultPersistenceConfiguration cannot be null");
    }
    lockFile = new File(rootDirectory, ".lock");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void start(final ServiceProvider<Service> serviceProvider) {
    internalStart();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void startForMaintenance(ServiceProvider<MaintainableService> serviceProvider) {
    internalStart();
  }

  private void internalStart() {
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

  /**
   * {@inheritDoc}
   */
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

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean handlesResourceType(ResourceType<?> resourceType) {
    return ResourceType.Core.DISK.equals(resourceType);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Collection<ServiceConfiguration<?>> additionalConfigurationsForPool(String alias, ResourcePool pool) throws CachePersistenceException {
    if (!ResourceType.Core.DISK.equals(pool.getType())) {
      throw new IllegalArgumentException("Pool is of wrong resource type. Disk expected, received " + pool.getType());
    }
    if (!pool.isPersistent()) {
      try {
        destroy(alias);
      } catch (CachePersistenceException cpex) {
        throw new RuntimeException("Unable to clean-up persistence space for non-restartable cache " + alias, cpex);
      }
    }
    try {
      return Collections.<ServiceConfiguration<?>>singleton(getOrCreatePersistenceSpace(alias));
    } catch (CachePersistenceException cpex) {
      throw new RuntimeException("Unable to create persistence space for cache " + alias, cpex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PersistenceSpaceIdentifier getOrCreatePersistenceSpace(String name) throws CachePersistenceException {
    PersistenceSpaceIdentifier existingSpace = knownPersistenceSpaces.get(name);
    if (existingSpace != null) {
      return existingSpace;
    }

    DefaultPersistenceSpaceIdentifier newSpace = new DefaultPersistenceSpaceIdentifier(getDirectoryFor(name));
    if ((existingSpace = knownPersistenceSpaces.putIfAbsent(name, newSpace)) == null) {
      try {
        create(newSpace.getDirectory());
      } catch (IOException e) {
        knownPersistenceSpaces.remove(name, newSpace);
        throw new CachePersistenceException("Unable to create persistence space for " + name, e);
      }
      return newSpace;
    } else {
      return existingSpace;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void destroy(String name) throws CachePersistenceException {
    DefaultPersistenceSpaceIdentifier space = knownPersistenceSpaces.remove(name);
    if (space == null) {
      destroy(name, new DefaultPersistenceSpaceIdentifier(getDirectoryFor(name)), true);
    } else {
      destroy(name, space, true);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void destroyAll() {
    if(recursiveDeleteDirectoryContent(rootDirectory)){
      LOGGER.debug("Destroyed all file based persistence contexts");
    } else {
      LOGGER.warn("Could not delete all file based persistence contexts");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public FileBasedPersistenceContext createPersistenceContextWithin(PersistenceSpaceIdentifier space, String name) throws CachePersistenceException {
    if (knownPersistenceSpaces.containsValue(space)) {
      File directory = new File(((DefaultPersistenceSpaceIdentifier) space).getDirectory(), name);
      try {
        create(directory);
      } catch (IOException ex) {
        throw new CachePersistenceException("Unable to create persistence context for " + name + " in " + space);
      }
      return new DefaultFileBasedPersistenceContext(directory);
    } else {
      throw new CachePersistenceException("Unknown space: " + space);
    }
  }

  File getLockFile() {
    return lockFile;
  }

  private File getDirectoryFor(String identifier) {
    File directory = new File(rootDirectory, safeIdentifier(identifier));

    for (File parent = directory.getParentFile(); parent != null; parent = parent.getParentFile()) {
      if (rootDirectory.equals(parent)) {
        return directory;
      }
    }

    throw new IllegalArgumentException("Attempted to access file outside the persistence path");
  }

  private static void create(File directory) throws IOException, CachePersistenceException {
    if (directory.isDirectory()) {
      LOGGER.debug("Reusing {}", directory.getAbsolutePath());
    } else if (directory.mkdir()) {
      LOGGER.debug("Created {}", directory.getAbsolutePath());
    } else {
      throw new CachePersistenceException("Unable to create or reuse directory: " + directory.getAbsolutePath());
    }
  }

  private static void destroy(String identifier, DefaultPersistenceSpaceIdentifier fileBasedPersistenceContext, boolean verbose) {
    if (verbose) {
      LOGGER.debug("Destroying file based persistence context for {}", identifier);
    }
    if (fileBasedPersistenceContext.getDirectory().exists() && !tryRecursiveDelete(fileBasedPersistenceContext.getDirectory())) {
      if (verbose) {
        LOGGER.warn("Could not delete directory for context {}", identifier);
      }
    }
  }

  private static boolean recursiveDeleteDirectoryContent(File file) {
    File[] contents = file.listFiles();
    if (contents == null) {
      throw new IllegalArgumentException("File " + file.getAbsolutePath() + " is not a directory");
    } else {
      boolean deleteSuccessful = true;
      for (File f : contents) {
        deleteSuccessful &= tryRecursiveDelete(f);
      }
      return deleteSuccessful;
    }
  }

  private static boolean recursiveDelete(File file) {
    Deque<File> toDelete = new ArrayDeque<File>();
    toDelete.push(file);
    while (!toDelete.isEmpty()) {
      File target = toDelete.pop();
      File[] contents = target.listFiles();
      if (contents == null || contents.length == 0) {
        if (target.exists() && !target.delete()) {
          return false;
        }
      } else {
        toDelete.push(target);
        for (File f : contents) {
          toDelete.push(f);
        }
      }
    }
    return true;
  }

  @SuppressFBWarnings("DM_GC")
  private static boolean tryRecursiveDelete(File file) {
    boolean interrupted = false;
    try {
      for (int i = 0; i < 5; i++) {
        if (recursiveDelete(file) || !isWindows()) {
          return true;
        } else {
          System.gc();
          System.runFinalization();

          try {
            Thread.sleep(50);
          } catch (InterruptedException e) {
            interrupted = true;
          }
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
    return false;
 }

  private static boolean isWindows() {
    return System.getProperty("os.name").toLowerCase(Locale.ENGLISH).contains("windows");
  }

  /**
   * sanitize a name for valid file or directory name
   *
   * @param name the name to sanitize
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

  /**
   * {@inheritDoc}
   */
  @Override
  public void create() {
    //no-op
  }

  private static abstract class FileHolder {
    final File directory;

    FileHolder(File directory) {
      this.directory = directory;
    }

    public File getDirectory() {
      return directory;
    }

  }
  private static class DefaultPersistenceSpaceIdentifier extends FileHolder implements PersistenceSpaceIdentifier {

    DefaultPersistenceSpaceIdentifier(File directory) {
      super(directory);
    }

    @Override
    public Class<LocalPersistenceService> getServiceType() {
      return LocalPersistenceService.class;
    }
  }

  private static class DefaultFileBasedPersistenceContext extends FileHolder implements FileBasedPersistenceContext {

    DefaultFileBasedPersistenceContext(File directory) {
      super(directory);
    }
  }
}
