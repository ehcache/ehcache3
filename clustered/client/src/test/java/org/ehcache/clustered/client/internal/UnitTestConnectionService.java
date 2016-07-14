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


import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLockEntityClientService;
import org.ehcache.clustered.lock.server.VoltronReadWriteLockServerEntityService;
import org.ehcache.clustered.server.EhcacheServerEntityService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionException;
import org.terracotta.connection.ConnectionPropertyNames;
import org.terracotta.connection.ConnectionService;
import org.terracotta.entity.EntityClientService;
import org.terracotta.entity.EntityMessage;
import org.terracotta.entity.EntityResponse;
import org.terracotta.entity.EntityServerService;
import org.terracotta.entity.ServiceProvider;
import org.terracotta.entity.ServiceProviderConfiguration;
import org.terracotta.entity.map.TerracottaClusteredMapClientService;
import org.terracotta.entity.map.server.TerracottaClusteredMapService;
import org.terracotta.offheapresource.OffHeapResourcesConfiguration;
import org.terracotta.offheapresource.OffHeapResourcesProvider;
import org.terracotta.offheapresource.config.MemoryUnit;
import org.terracotta.offheapresource.config.OffheapResourcesType;
import org.terracotta.offheapresource.config.ResourceType;
import org.terracotta.passthrough.PassthroughServer;

/**
 * A {@link ConnectionService} implementation used to simulate Voltron server connections for unit testing purposes.
 * In common usage, this class:
 * <ol>
 *   <li>is loaded once per JVM (potentially covering all unit tests in a Gradle test run)</li>
 *   <li>is accessed through a
 *       {@link org.terracotta.connection.ConnectionFactory#connect(URI, Properties) ConnectionFactory.connect(URI, Properties)}
 *       method call
 *   </li>
 *   <li>is instantiated by {@link java.util.ServiceLoader} through an {@code Iterator} obtained over
 *       configured {@link ConnectionService} classes
 *   </li>
 *   <li>is selected through calls to the {@link ConnectionService#handlesURI(URI)} method</li>
 *   <li>once selected, is used to obtain a {@link Connection} through the
 *       {@link ConnectionService#connect(URI, Properties)} method</li>
 * </ol>
 * Even though a unit test may make a direct reference to this class for configuration purposes, a
 * {@code ServiceLoader} provider-configuration file for {@code org.terracotta.connection.ConnectionService}
 * referring to this class must be available in the class path.
 *
 * <p>
 *   For use, a unit test should define {@link org.junit.Before @Before} and {@link org.junit.After @After}
 *   methods as in the following examples:
 *   <pre><code>
 * &#64;Before
 * public void definePassthroughServer() throws Exception {
 *   UnitTestConnectionService.add(<i>CLUSTER_URI</i>,
 *       new PassthroughServerBuilder()
 *           .resource("primary-server-resource", 64, MemoryUnit.MB)
 *           .resource("secondary-server-resource", 64, MemoryUnit.MB)
 *           .build());
 * }
 *
 * &#64;After
 * public void removePassthroughServer() throws Exception {
 *   UnitTestConnectionService.remove(<i>CLUSTER_URI</i>);
 * }
 *   </code></pre>
 *
 *   If your configuration uses no server resources, none need be defined.  The {@link PassthroughServerBuilder}
 *   can also add Voltron server & client services and service providers.
 * </p>
 * <p>
 *   Tests needing direct access to a {@link Connection} can obtain a connection using the following:
 *   <pre><code>
 * Connection connection = new UnitTestConnectionService().connect(<i>CLUSTER_URI</i>, new Properties());
 *   </code></pre>
 *   after the server has been added to {@code UnitTestConnectionService}.  Ideally, this connection should
 *   be explicitly closed when no longer needed but {@link #remove} closes any remaining connections opened
 *   through {@link #connect(URI, Properties)}.
 * </p>
 *
 * @see PassthroughServerBuilder
 */
public class UnitTestConnectionService implements ConnectionService {

  private static final Logger LOGGER = LoggerFactory.getLogger(UnitTestConnectionService.class);

  private static final Map<URI, ServerDescriptor> SERVERS = new HashMap<URI, ServerDescriptor>();

  /**
   * Adds a {@link PassthroughServer} if, and only if, a mapping for the {@code URI} supplied does not
   * already exist.  The server is started as it is added.
   *
   * @param uri the {@code URI} for the server; only the <i>scheme</i>, <i>host</i>, and <i>port</i>
   *            contribute to the server identification
   * @param server the {@code PassthroughServer} instance to use for connections to {@code uri}
   */
  public static void add(URI uri, PassthroughServer server) {
    URI keyURI = createKey(uri);
    if (SERVERS.containsKey(keyURI)) {
      throw new AssertionError("Server at " + uri + " already provided; use remove() to remove");
    }

    SERVERS.put(keyURI, new ServerDescriptor(server));
    server.start(true, false);
    LOGGER.info("Started PassthroughServer at {}", keyURI);
  }

  /**
   * Adds a {@link PassthroughServer} if, and only if, a mapping for the URI supplied does not
   * already exist.  The server is started as it is added.
   *
   * @param uri the URI, in string form, for the server; only the <i>scheme</i>, <i>host</i>, and <i>port</i>
   *            contribute to the server identification
   * @param server the {@code PassthroughServer} instance to use for connections to {@code uri}
   */
  public static void add(String uri, PassthroughServer server) {
    add(URI.create(uri), server);
  }

  /**
   * Removes the {@link PassthroughServer} previously associated with the {@code URI} provided.  The
   * server is stopped as it is removed.  In addition to stopping the server, all open {@link Connection}
   * instances created through the {@link #connect(URI, Properties)} method are closed; this is done to
   * clean up the threads started in support of each open connection.
   *
   * @param uri the {@code URI} for which the server is removed
   *
   * @return the removed {@code PassthroughServer}
   */
  public static PassthroughServer remove(URI uri) {
    URI keyURI = createKey(uri);
    ServerDescriptor serverDescriptor = SERVERS.remove(keyURI);
    if (serverDescriptor != null) {
      serverDescriptor.server.stop();
      LOGGER.info("Stopped PassthroughServer at {}", keyURI);

      for (Connection connection : serverDescriptor.getConnections().keySet()) {
        try {
          LOGGER.warn("Force close {}", formatConnectionId(connection));
          connection.close();
        } catch (AssertionError e) {
          // Ignored -- https://github.com/Terracotta-OSS/terracotta-apis/issues/102
        } catch (IOException e) {
          // Ignored
        }
      }
      return serverDescriptor.server;
    } else {
      return null;
    }
  }

  /**
   * Removes the {@link PassthroughServer} previously associated with the URI provided.  The server
   * is stopped as it is removed.
   *
   * @param uri the URI, in string form, for which the server is removed
   *
   * @return the removed {@code PassthroughServer}
   */
  public static PassthroughServer remove(String uri) {
    return remove(URI.create(uri));
  }

  /**
   * A builder for a new {@link PassthroughServer} instance.  If no services are added using
   * {@link #serverEntityService(EntityServerService)} or {@link #clientEntityService(EntityClientService)},
   * this builder defines the following services for each {@code PassthroughServer} built:
   * <ul>
   *   <li>{@link EhcacheServerEntityService}</li>
   *   <li>{@link EhcacheClientEntityService}</li>
   *   <li>{@link VoltronReadWriteLockServerEntityService}</li>
   *   <li>{@link VoltronReadWriteLockEntityClientService}</li>
   *   <li>{@link TerracottaClusteredMapClientService}</li>
   *   <li>{@link TerracottaClusteredMapService}</li>
   * </ul>
   */
  @SuppressWarnings("unused")
  public static final class PassthroughServerBuilder {
    private final List<EntityServerService<?, ?>> serverEntityServices = new ArrayList<EntityServerService<?, ?>>();
    private final List<EntityClientService<?, ?, ? extends EntityMessage, ? extends EntityResponse>> clientEntityServices =
        new ArrayList<EntityClientService<?, ?, ? extends EntityMessage, ? extends EntityResponse>>();
    private final Map<ServiceProvider, ServiceProviderConfiguration> serviceProviders =
        new IdentityHashMap<ServiceProvider, ServiceProviderConfiguration>();

    private final OffheapResourcesType resources = new OffheapResourcesType();

    public PassthroughServerBuilder resource(String resourceName, int size, org.ehcache.config.units.MemoryUnit unit) {
      return this.resource(resourceName, size, convert(unit));
    }

    private MemoryUnit convert(org.ehcache.config.units.MemoryUnit unit) {
      MemoryUnit convertedUnit;
      switch (unit) {
        case B:
          convertedUnit = MemoryUnit.B;
          break;
        case KB:
          convertedUnit = MemoryUnit.K_B;
          break;
        case MB:
          convertedUnit = MemoryUnit.MB;
          break;
        case GB:
          convertedUnit = MemoryUnit.GB;
          break;
        case TB:
          convertedUnit = MemoryUnit.TB;
          break;
        case PB:
          convertedUnit = MemoryUnit.PB;
          break;
        default:
          throw new UnsupportedOperationException("Unrecognized unit " + unit);
      }
      return convertedUnit;
    }

    private PassthroughServerBuilder resource(String resourceName, int size, MemoryUnit unit) {
      final ResourceType resource = new ResourceType();
      resource.setName(resourceName);
      resource.setUnit(unit);
      resource.setValue(BigInteger.valueOf((long)size));
      this.resources.getResource().add(resource);
      return this;
    }

    public PassthroughServerBuilder serviceProvider(ServiceProvider serviceProvider, ServiceProviderConfiguration configuration) {
      this.serviceProviders.put(serviceProvider, configuration);
      return this;
    }

    public PassthroughServerBuilder serverEntityService(EntityServerService<?, ?> service) {
      this.serverEntityServices.add(service);
      return this;
    }

    public PassthroughServerBuilder clientEntityService(EntityClientService<?, ?, ? extends EntityMessage, ? extends EntityResponse> service) {
      this.clientEntityServices.add(service);
      return this;
    }

    public PassthroughServer build() {
      PassthroughServer newServer = new PassthroughServer();

      /*
       * If services have been specified, don't establish the "defaults".
       */
      if (serverEntityServices.isEmpty() && clientEntityServices.isEmpty()) {
        newServer.registerServerEntityService(new EhcacheServerEntityService());
        newServer.registerClientEntityService(new EhcacheClientEntityService());
        newServer.registerServerEntityService(new VoltronReadWriteLockServerEntityService());
        newServer.registerClientEntityService(new VoltronReadWriteLockEntityClientService());
        newServer.registerClientEntityService(new TerracottaClusteredMapClientService());
        newServer.registerServerEntityService(new TerracottaClusteredMapService());
      }

      for (EntityServerService<?, ?> service : serverEntityServices) {
        newServer.registerServerEntityService(service);
      }

      for (EntityClientService<?, ?, ? extends EntityMessage, ? extends EntityResponse> service : clientEntityServices) {
        newServer.registerClientEntityService(service);
      }

      if (!this.resources.getResource().isEmpty()) {
        newServer.registerServiceProvider(new OffHeapResourcesProvider(), new OffHeapResourcesConfiguration(this.resources));
      }

      for (Map.Entry<ServiceProvider, ServiceProviderConfiguration> entry : serviceProviders.entrySet()) {
        newServer.registerServiceProvider(entry.getKey(), entry.getValue());
      }

      return newServer;
    }
  }

  public static Collection<Properties> getConnectionProperties(URI uri) {
    ServerDescriptor serverDescriptor = SERVERS.get(createKey(uri));
    if (serverDescriptor != null) {
      return serverDescriptor.getConnections().values();
    } else {
      return Collections.emptyList();
    }
  }

  @Override
  public boolean handlesURI(URI uri) {
    checkURI(uri);
    return SERVERS.containsKey(uri);
  }

  @Override
  public Connection connect(URI uri, Properties properties) throws ConnectionException {
    checkURI(uri);

    ServerDescriptor serverDescriptor = SERVERS.get(uri);
    if (serverDescriptor == null) {
      throw new IllegalArgumentException("No server available for " + uri);
    }

    String name = properties.getProperty(ConnectionPropertyNames.CONNECTION_NAME);
    if(name == null) {
      name = "Ehcache:UNKNOWN";
    }
    Connection connection = serverDescriptor.server.connectNewClient(name);
    serverDescriptor.add(connection, properties);

    LOGGER.info("Client opened {} to PassthroughServer at {}", formatConnectionId(connection), uri);

    /*
     * Uses a Proxy around Connection so closed connections can be removed from the ServerDescriptor.
     */
    return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
        new Class[] { Connection.class },
        new ConnectionInvocationHandler(serverDescriptor, connection));
  }

  /**
   * Ensures that the {@code URI} presented conforms to the value used to locate a server.
   *
   * @param requestURI the {@code URI} to check
   *
   * @throws IllegalArgumentException if the {@code URI} is not equal to the {@code URI} as reformed
   *          by {@link #createKey(URI)}
   *
   * @see #checkURI(URI)
   */
  private static void checkURI(URI requestURI) throws IllegalArgumentException {
    if (!requestURI.equals(createKey(requestURI))) {
      throw new IllegalArgumentException("Connection URI contains user-info, path, query, and/or fragment");
    }
  }

  /**
   * Creates a "key" {@code URI} by dropping the <i>user-info</i>, <i>path</i>, <i>query</i>, and <i>fragment</i>
   * portions of the {@code URI}.
   *
   * @param requestURI the {@code URI} for which the key is to be generated
   *
   * @return a {@code URI} instance with the <i>user-info</i>, <i>path</i>, <i>query</i>, and <i>fragment</i> discarded
   */
  private static URI createKey(URI requestURI) {
    try {
      URI keyURI = requestURI.parseServerAuthority();
      return new URI(keyURI.getScheme(), null, keyURI.getHost(), keyURI.getPort(), null, null, null);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private static final class ServerDescriptor {
    private final PassthroughServer server;
    private final Map<Connection, Properties> connections = new IdentityHashMap<Connection, Properties>();

    ServerDescriptor(PassthroughServer server) {
      this.server = server;
    }

    synchronized Map<Connection, Properties> getConnections() {
      return new IdentityHashMap<Connection, Properties>(this.connections);
    }

    synchronized void add(Connection connection, Properties properties) {
      this.connections.put(connection, properties);
    }

    synchronized void remove(Connection connection) {
      this.connections.remove(connection);
    }
  }

  /**
   * An {@link InvocationHandler} for a proxy over a {@link Connection} instance catch
   * connection closure for managing the server connection collection.
   */
  private static final class ConnectionInvocationHandler implements InvocationHandler {

    private final ServerDescriptor serverDescriptor;
    private final Connection connection;

    ConnectionInvocationHandler(ServerDescriptor serverDescriptor, Connection connection) {
      this.serverDescriptor = serverDescriptor;
      this.connection = connection;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      if (method.getName().equals("close")) {
        serverDescriptor.remove(connection);
        LOGGER.info("Client closed {}", formatConnectionId(connection));
      }
      try {
        return method.invoke(connection, args);
      } catch (InvocationTargetException e) {
        throw e.getCause();
      }
    }
  }

  private static CharSequence formatConnectionId(Connection connection) {
    return connection.getClass().getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(connection));
  }
}
