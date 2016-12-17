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

package org.ehcache.core.spi.service;

import org.ehcache.CachePersistenceException;
import org.ehcache.spi.service.MaintainableService;

import java.io.File;

/**
 * Service that provides isolated persistence spaces to any service that requires it
 * under the local root directory.
 */
public interface LocalPersistenceService extends MaintainableService {

  /**
   * Creates a logical safe directory space for the owner and returns an identifying space Id.
   *
   * @param owner Service owner that owns the safe space.
   * @param name Identifying name for the space.
   *
   * @return Opaque Identifier that can be used to identify the safe space.
   */
  SafeSpaceIdentifier createSafeSpaceIdentifier(String owner, String name);

  /**
   * Creates the safe space represented by {@code safeSpaceId}, if it does not exist in the underlying physical space.
   *
   * @param safeSpaceId Identifier to the created logical space on which the physical space needs to be created
   * @throws CachePersistenceException If the space cannot be created or found, due to system errors
   */
  void createSafeSpace(SafeSpaceIdentifier safeSpaceId) throws CachePersistenceException;

  /**
   * Destroys the safe space.
   *
   * @param safeSpaceId Safe space identifier.
   * @param verbose Log more information.
   */
  void destroySafeSpace(SafeSpaceIdentifier safeSpaceId, boolean verbose);

  /**
   * Destroys all safe spaces provided to this owner.
   *
   * @param owner owner of safe spaces.
   */
  void destroyAll(String owner);

  /**
   * Identifier to the logical safe space
   */
  interface SafeSpaceIdentifier {
    /**
     * Represents the root directory of the given logical safe space.
     * <P>
     *   Note that the directory represented by {@code File} may or may not be created in the physical space.
     *   The existence of the physical space depends on whether the {@code createSafeSpace} method was invoked
     *   for the space at some time in the past or not.
     * </P>
     *
     * @return Root directory of the safe space.
     */
    File getRoot();
  }
}