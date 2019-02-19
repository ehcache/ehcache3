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

import org.ehcache.CachePersistenceException;
import org.ehcache.core.spi.service.LocalPersistenceService;
import org.ehcache.impl.config.persistence.DefaultPersistenceConfiguration;
import org.ehcache.spi.service.MaintainableService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;

import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.deleteIfExists;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.isDirectory;
import static java.nio.file.Files.isWritable;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.ehcache.impl.persistence.FileUtils.safeIdentifier;
import static org.ehcache.impl.persistence.FileUtils.tryRecursiveDelete;
import static org.ehcache.impl.persistence.FileUtils.validateName;

/**
 * Implements the local persistence service that provides individual sub-spaces for different
 * services.
 */
public class DefaultLocalPersistenceService implements LocalPersistenceService {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultLocalPersistenceService.class);

  private final Path rootDirectory;
  private final Path lockFile;

  private FileLock lock;
  private FileChannel rwChannel;
  private boolean started;

  /**
   * Creates a new service instance using the provided configuration.
   *
   * @param persistenceConfiguration the configuration to use
   */
  public DefaultLocalPersistenceService(final DefaultPersistenceConfiguration persistenceConfiguration) {
    if(persistenceConfiguration != null) {
      rootDirectory = persistenceConfiguration.getRootDirectoryPath();
    } else {
      throw new NullPointerException("DefaultPersistenceConfiguration cannot be null");
    }
    lockFile = rootDirectory.resolve(".lock");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void start(final ServiceProvider<Service> serviceProvider) {
    internalStart();
  }

  @Override
  public synchronized void startForMaintenance(ServiceProvider<? super MaintainableService> serviceProvider, MaintenanceScope maintenanceScope) {
    internalStart();
  }

  private void internalStart() {
    if (!started) {
      try {
        if (!isWritable(createDirectories(rootDirectory))) {
          throw new IllegalArgumentException("Persistence directory isn't writable: " + rootDirectory.toAbsolutePath());
        }
      } catch (IOException e) {
        throw new IllegalArgumentException("Persistence directory couldn't be created: " + rootDirectory.toAbsolutePath());
      }
      try {
        rwChannel = FileChannel.open(lockFile, WRITE, CREATE);
        try {
          lock = rwChannel.tryLock();
        } catch (Throwable t) {
          try {
            rwChannel.close();
          } finally {
            deleteIfExists(lockFile);
          }
          throw t;
        }
      } catch (OverlappingFileLockException e) {
        throw new RuntimeException("Persistence directory already locked by this process: " + rootDirectory.toAbsolutePath(), e);
      } catch (IOException e) {
        throw new RuntimeException("Persistence directory couldn't be locked: " + rootDirectory.toAbsolutePath(), e);
      }
      if (lock == null) {
        throw new RuntimeException("Persistence directory already locked by another process: " + rootDirectory.toAbsolutePath());
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
        try {
          lock.release();
        } finally {
          try {
            rwChannel.close();
          } finally {
            deleteIfExists(lockFile);
          }
        }
      } catch (IOException e) {
        throw new RuntimeException("Couldn't unlock rootDir: " + rootDirectory.toAbsolutePath(), e);
      }
      started = false;
      LOGGER.debug("RootDirectory Unlocked");
    }
  }

  Path getLockFile() {
    return lockFile;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SafeSpaceIdentifier createSafeSpaceIdentifier(String owner, String identifier) {
    SafeSpace ss = createSafeSpaceLogical(validateName(owner), identifier);

    if (ss.directory.toAbsolutePath().normalize().startsWith(rootDirectory.toAbsolutePath().normalize())) {
      return new DefaultSafeSpaceIdentifier(ss);
    } else {
      throw new IllegalArgumentException("Attempted to access data outside the persistence path");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void createSafeSpace(SafeSpaceIdentifier safeSpaceId) throws CachePersistenceException {
    if (safeSpaceId == null || !(safeSpaceId instanceof DefaultSafeSpaceIdentifier)) {
      // this cannot happen..if identifier created before creating physical space..
      throw new AssertionError("Invalid safe space identifier. Identifier not created");
    }
    SafeSpace ss = ((DefaultSafeSpaceIdentifier) safeSpaceId).safeSpace;

    try {
      createDirectories(ss.directory);
    } catch (IOException e) {
      throw new CachePersistenceException("Unable to create directory: " + ss.directory.toAbsolutePath());
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void destroySafeSpace(SafeSpaceIdentifier safeSpaceId, boolean verbose) {
    if (safeSpaceId == null || !(safeSpaceId instanceof DefaultSafeSpaceIdentifier)) {
      // this cannot happen..if identifier created before creating/destroying physical space..
      throw new AssertionError("Invalid safe space identifier. Identifier not created");
    }
    SafeSpace ss = ((DefaultSafeSpaceIdentifier) safeSpaceId).safeSpace;
    destroy(ss, verbose);
  }

  /**
   * {@inheritDoc}
   */
  public void destroyAll(String owner) {
    Path ownerDirectory = rootDirectory.resolve(owner);
    if (isDirectory(ownerDirectory)) {
      if (tryRecursiveDelete(ownerDirectory)) {
        LOGGER.debug("Destroyed all file based persistence contexts owned by {}", owner);
      } else {
        LOGGER.warn("Could not delete all file based persistence contexts owned by {}", owner);
      }
    }
  }

  private void destroy(SafeSpace ss, boolean verbose) {
    if (verbose) {
      LOGGER.debug("Destroying file based persistence context for {}", ss.identifier);
    }
    if (exists(ss.directory) && !tryRecursiveDelete(ss.directory)) {
      if (verbose) {
        LOGGER.warn("Could not delete directory for context {}", ss.identifier);
      }
    }
  }


  private SafeSpace createSafeSpaceLogical(String owner, String identifier) {
    Path ownerDirectory = rootDirectory.resolve(owner);
    Path directory = ownerDirectory.resolve(safeIdentifier(identifier));
    return new SafeSpace(identifier, directory);
  }

  private static final class SafeSpace {
    private final String identifier;
    private final Path directory;

    private SafeSpace(String identifier, Path directory) {
      this.directory = directory;
      this.identifier = identifier;
    }
  }


  private static final class DefaultSafeSpaceIdentifier implements SafeSpaceIdentifier {
    private final SafeSpace safeSpace;

    private DefaultSafeSpaceIdentifier(SafeSpace safeSpace) {
      this.safeSpace = safeSpace;
    }

    @Override
    public String toString() {
      return safeSpace.identifier;
    }

    @Override
    public Path getRootPath() {
      return safeSpace.directory;
    }
  }
}
