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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.HashSet;
import java.util.Set;

/**
 * Manager class to handle disk store path. CacheManager has a reference to this manager.
 *
 * @author hhuynh
 */
public final class DiskStorePathManager {
  /**
   * If the CacheManager needs to resolve a conflict with the disk path, it will create a
   * subdirectory in the given disk path with this prefix followed by a number. The presence of this
   * name is used to determined whether it makes sense for a persistent DiskStore to be loaded. Loading
   * persistent DiskStores will only have useful semantics where the diskStore path has not changed.
   */
  private static final String AUTO_DISK_PATH_DIRECTORY_PREFIX = "ehcache_auto_created";
  private static final Logger LOG = LoggerFactory.getLogger(DiskStorePathManager.class);
  private static final String LOCK_FILE_NAME = ".ehcache-diskstore.lock";

  private static final String DEFAULT_PATH = System.getProperty("java.io.tmpdir");

  private static final int DEL = 0x7F;
  private static final char ESCAPE = '%';
  private static final Set<Character> ILLEGALS = new HashSet<Character>();

  private final File initialPath;
  private final boolean defaultPath;

  private volatile DiskStorePath path;

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

  /**
   * Create a diskstore path manager with provided initial path.
   *
   * @param initialPath
   */
  public DiskStorePathManager(String initialPath) {
    this.initialPath = new File(initialPath);
    this.defaultPath = false;
  }

  /**
   * Create a diskstore path manager using the default path.
   */
  public DiskStorePathManager() {
    this.initialPath = new File(DEFAULT_PATH);
    this.defaultPath = true;
  }

  /**
   * Resolve and lock this disk store path if the resultant path contains the supplied file.
   *
   * @param file file to check for
   * @return {@code true} if the file existed and the path was successfully locked
   */
  public boolean resolveAndLockIfExists(String file) {
    if (path != null) {
      return getFile(file).exists();
    }

    synchronized (this) {
      if (path != null) {
        return getFile(file).exists();
      }

      // ensure disk store path exists
      if (!initialPath.isDirectory()) {
        return false;
      } else if (!new File(initialPath, file).exists()) {
        return false;
      } else {
        try {
          path = new DiskStorePath(initialPath, false, defaultPath);
        } catch (DiskstoreNotExclusiveException e) {
          throw new RuntimeException(e);
        }

        LOG.debug("Using diskstore path {}", path.getDiskStorePath());
        LOG.debug("Holding exclusive lock on {}", path.getLockFile());
        return true;
      }
    }
  }

  private void resolveAndLockIfNeeded(boolean allowAutoCreate) throws DiskstoreNotExclusiveException {
    if (path != null) {
      return;
    }

    synchronized (this) {
      if (path != null) {
        return;
      }

      File candidate = initialPath;

      boolean autoCreated = false;
      while (true) {
        // ensure disk store path exists
        if (!candidate.isDirectory() && !candidate.mkdirs()) {
          throw new RuntimeException("Disk store path can't be created: " + candidate);
        }

        try {
          path = new DiskStorePath(candidate, autoCreated, !autoCreated && defaultPath);
          break;
        } catch (DiskstoreNotExclusiveException e) {
          if (!allowAutoCreate) {
            throw e;
          }

          autoCreated = true;
          try {
            candidate = File.createTempFile(AUTO_DISK_PATH_DIRECTORY_PREFIX, "diskstore", initialPath);
            // we want to create a directory with this temp name so deleting the file first
            candidate.delete();
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
        }
      }

      if (autoCreated) {
        LOG.warn("diskStorePath '" + initialPath
            + "' is already used by an existing CacheManager either in the same VM or in a different process.\n"
            + "The diskStore path for this CacheManager will be set to " + candidate + ".\nTo avoid this"
            + " warning consider using the CacheManager factory methods to create a singleton CacheManager "
            + "or specifying a separate ehcache configuration (ehcache.xml) for each CacheManager instance.");
      }

      LOG.debug("Using diskstore path {}", path.getDiskStorePath());
      LOG.debug("Holding exclusive lock on {}", path.getLockFile());
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

  private static void deleteFile(File f) {
    if (!f.delete()) {
      LOG.debug("Failed to delete file {}", f.getAbsolutePath());
    }
  }

  /**
   * Was this path auto-created (ie. the result of a collision)
   *
   * @return true if path is auto created
   */
  public boolean isAutoCreated() {
    DiskStorePath diskStorePath = path;
    if (diskStorePath == null) {
      throw new IllegalStateException();
    }

    return diskStorePath.isAutoCreated();
  }

  /**
   * Was this path sourced from the default value.
   *
   * @return true if path is the default
   */
  public boolean isDefault() {
    DiskStorePath diskStorePath = path;
    if (diskStorePath == null) {
      throw new IllegalStateException();
    }

    return diskStorePath.isDefault();
  }

  /**
   * release the lock file used for collision detection
   * should be called when cache manager shutdowns
   */
  public synchronized void releaseLock() {
    try {
      if (path != null) {
        path.unlock();
      }
    } finally {
      path = null;
    }
  }

  /**
   * Get a file object for the given cache-name and suffix
   *
   * @param cacheName the cache name
   * @param suffix    a file suffix
   * @return a file object
   */
  public File getFile(String cacheName, String suffix) {
    return getFile(safeName(cacheName) + suffix);
  }

  /**
   * Get a file object for the given name
   *
   * @param name the file name
   * @return a file object
   */
  public File getFile(String name) {
    try {
      resolveAndLockIfNeeded(true);
    } catch (DiskstoreNotExclusiveException e) {
      throw new RuntimeException(e);
    }

    File diskStorePath = path.getDiskStorePath();

    File file = new File(diskStorePath, name);
    for (File parent = file.getParentFile(); parent != null; parent = parent.getParentFile()) {
      if (diskStorePath.equals(parent)) {
        return file;
      }
    }

    throw new IllegalArgumentException("Attempted to access file outside the disk-store path");
  }

  /**
   * Exception class thrown when a diskstore path collides with an existing one
   */
  private static class DiskstoreNotExclusiveException extends Exception {

    /**
     * Constructor for the DiskstoreNotExclusiveException object.
     */
    public DiskstoreNotExclusiveException() {
      super();
    }

    /**
     * Constructor for the DiskstoreNotExclusiveException object.
     *
     * @param message the exception detail message
     */
    public DiskstoreNotExclusiveException(String message) {
      super(message);
    }
  }

  /**
   * Resolved path and lock details
   */
  private static class DiskStorePath {
    private final FileLock directoryLock;
    private final File lockFile;
    private final File diskStorePath;
    private final boolean autoCreated;
    private final boolean defaultPath;

    DiskStorePath(File path, boolean autoCreated, boolean defaultPath) throws DiskstoreNotExclusiveException {
      this.diskStorePath = path;
      this.autoCreated = autoCreated;
      this.defaultPath = defaultPath;

      lockFile = new File(path.getAbsoluteFile(), LOCK_FILE_NAME);
      lockFile.deleteOnExit();

      FileLock dirLock;
      try {
        lockFile.createNewFile();
        if (!lockFile.exists()) {
          throw new AssertionError("Failed to create lock file " + lockFile);
        }
        FileChannel lockFileChannel = new RandomAccessFile(lockFile, "rw").getChannel();
        dirLock = lockFileChannel.tryLock();
      } catch (OverlappingFileLockException ofle) {
        dirLock = null;
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }

      if (dirLock == null) {
        throw new DiskstoreNotExclusiveException(path.getAbsolutePath() + " is not exclusive.");
      }

      this.directoryLock = dirLock;
    }

    boolean isAutoCreated() {
      return autoCreated;
    }

    boolean isDefault() {
      return defaultPath;
    }

    File getDiskStorePath() {
      return diskStorePath;
    }

    File getLockFile() {
      return lockFile;
    }

    void unlock() {
      if (directoryLock != null && directoryLock.isValid()) {
        try {
          directoryLock.release();
          directoryLock.channel().close();
          deleteFile(lockFile);
        } catch (IOException e) {
          throw new RuntimeException("Failed to release disk store path's lock file:" + lockFile, e);
        }
      }


      if (autoCreated) {
        if (diskStorePath.delete()) {
          LOG.debug("Deleted directory " + diskStorePath.getName());
        }
      }
    }

    @Override
    public String toString() {
      return diskStorePath.getAbsolutePath();
    }
  }

}
