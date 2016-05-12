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

package org.ehcache.clustered.client.internal;


import java.math.BigInteger;
import java.net.URI;
import java.util.List;
import java.util.Properties;

import org.ehcache.clustered.server.EhcacheServerEntityService;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionException;
import org.terracotta.connection.ConnectionService;
import org.terracotta.consensus.entity.CoordinationServerEntityService;
import org.terracotta.consensus.entity.client.ClientCoordinationEntityService;
import org.terracotta.offheapresource.OffHeapResourcesConfiguration;
import org.terracotta.offheapresource.OffHeapResourcesProvider;
import org.terracotta.offheapresource.config.MemoryUnit;
import org.terracotta.offheapresource.config.OffheapResourcesType;
import org.terracotta.offheapresource.config.ResourceType;
import org.terracotta.passthrough.PassthroughServer;

public class UnitTestConnectionService implements ConnectionService {

  private static PassthroughServer createServer() throws Exception {
    PassthroughServer newServer = new PassthroughServer(true);
    newServer.registerServerEntityService(new EhcacheServerEntityService());
    newServer.registerClientEntityService(new EhcacheClientEntityService());
    newServer.registerServerEntityService(new CoordinationServerEntityService());
    newServer.registerClientEntityService(new ClientCoordinationEntityService());

    /*
     * Construct an off-heap resource configuration and register an OffHeapResourcesProvider.
     */
    OffheapResourcesType resources = new OffheapResourcesType();
    List<ResourceType> resourcesList = resources.getResource();
    resourcesList.add(defineResource("defaultResource", MemoryUnit.MB, 128L));
    resourcesList.add(defineResource("primary-server-resource", MemoryUnit.MB, 64L));
    resourcesList.add(defineResource("secondary-server-resource", MemoryUnit.MB, 64L));
    OffHeapResourcesConfiguration ohrpConfig = new OffHeapResourcesConfiguration(resources);
    newServer.registerServiceProvider(new OffHeapResourcesProvider(), ohrpConfig);

    newServer.start();
    return newServer;
  }

  private static ResourceType defineResource(String name, MemoryUnit unit, long size) {
    final ResourceType resource = new ResourceType();
    resource.setName(name);
    resource.setUnit(unit);
    resource.setValue(BigInteger.valueOf(size));
    return resource;
  }

  private static PassthroughServer server;
  static {
    try {
      server = createServer();
    } catch (Exception ex) {
      throw new AssertionError(ex);
    }
  }

  private static Properties connectionProperties;

  public static Properties getConnectionProperties() {
    return connectionProperties;
  }

  public static synchronized void reset() throws Exception {
    server.stop();
    server = createServer();
    connectionProperties = null;
  }

  public static synchronized PassthroughServer server() {
    return server;
  }

  @Override
  public boolean handlesURI(URI uri) {
    return uri.getHost().endsWith("example.com");
  }

  @Override
  public Connection connect(URI uri, Properties properties) throws ConnectionException {
    connectionProperties = properties;
    return server().connectNewClient();
  }
}
