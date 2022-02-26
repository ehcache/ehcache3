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
import org.terracotta.utilities.io.Files;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

import static org.ehcache.impl.persistence.FileUtils.createLocationIfRequiredAndVerify;
import static org.ehcache.impl.persistence.FileUtils.safeIdentifier;
import static org.ehcache.impl.persistence.FileUtils.tryRecursiveDelete;
import static org.ehcache.impl.persistence.FileUtils.validateName;

/**
 * Implements the local persistence service that provides individual sub-spaces for different
 * services.
 */
public class DefaultLocalPersistenceService implements LocalPersistenceService {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultLocalPersistenceService.class);

  private final File rootDirectory;
  private final File lockFile;

  private FileLock lock;
  private RandomAccessFile rw;
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

  @Override
  public synchronized void startForMaintenance(ServiceProvider<? super MaintainableService> serviceProvider, MaintenanceScope maintenanceScope) {
    internalStart();
  }

  private void internalStart() {
    if (!started) {
      createLocationIfRequiredAndVerify(rootDirectory);
      try {
        rw = new RandomAccessFile(lockFile, "rw");
      } catch (FileNotFoundException e) {
        // should not happen normally since we checked that everything is fine right above
        throw new RuntimeException(e);
      }
      try {
        lock = rw.getChannel().tryLock();
      } catch (OverlappingFileLockException e) {
        throw new RuntimeException("Persistence directory already locked by this process: " + rootDirectory.getAbsolutePath(), e);
      } catch (Exception e) {
        try {
          rw.close();
        } catch (IOException e1) {
          // ignore silently
        }
        throw new RuntimeException("Persistence directory couldn't be locked: " + rootDirectory.getAbsolutePath(), e);
      }
      if (lock == null) {
        throw new RuntimeException("Persistence directory already locked by another process: " + rootDirectory.getAbsolutePath());
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
        try {
          Files.delete(lockFile.toPath());
        } catch (IOException e) {
          LOGGER.debug("Lock file was not deleted {}.", lockFile.getPath());
        }
      } catch (IOException e) {
        throw new RuntimeException("Couldn't unlock rootDir: " + rootDirectory.getAbsolutePath(), e);
      }
      started = false;
      LOGGER.debug("RootDirectory Unlocked");
    }
  }

  File getLockFile() {
    return lockFile;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SafeSpaceIdentifier createSafeSpaceIdentifier(String owner, String identifier) {
    validateName(owner);
    SafeSpace ss = createSafeSpaceLogical(owner, identifier);

    for (File parent = ss.directory.getParentFile(); parent != null; parent = parent.getParentFile()) {
      if (rootDirectory.equals(parent)) {
        return new DefaultSafeSpaceIdentifier(ss);
      }
    }

    throw new IllegalArgumentException("Attempted to access file outside the persistence path");
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
    FileUtils.create(ss.directory.getParentFile());
    FileUtils.create(ss.directory);
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
    File ownerDirectory = new File(rootDirectory, owner);
    if (ownerDirectory.exists() && ownerDirectory.isDirectory()) {
      if (tryRecursiveDelete(ownerDirectory)) {
        LOGGER.debug("Destroyed all file based persistence contexts owned by {}", owner);
      } else {
        LOGGER.warn("Could not delete all file based persistence contexts owned by {}", owner);
      }
    } else {
      LOGGER.warn("Could not delete all file based persistence contexts owned by {} - is not a directory!", owner);
    }
  }

  private void destroy(SafeSpace ss, boolean verbose) {
    if (verbose) {
      LOGGER.debug("Destroying file based persistence context for {}", ss.identifier);
    }
    if (ss.directory.exists() && !tryRecursiveDelete(ss.directory)) {
      if (verbose) {
        LOGGER.warn("Could not delete directory for context {}", ss.identifier);
      }
    }
  }


  private SafeSpace createSafeSpaceLogical(String owner, String identifier) {
    File ownerDirectory = new File(rootDirectory, owner);
    File directory = new File(ownerDirectory, safeIdentifier(identifier));
    return new SafeSpace(identifier, directory);
  }

  private static final class SafeSpace {
    private final String identifier;
    private final File directory;

    private SafeSpace(String identifier, File directory) {
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
    public File getRoot() {
      return safeSpace.directory;
    }
  }
}
