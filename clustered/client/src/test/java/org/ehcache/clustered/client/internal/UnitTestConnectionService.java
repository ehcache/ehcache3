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


import java.net.URI;
import java.util.Properties;

import org.ehcache.clustered.server.EhcacheServerEntityService;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionException;
import org.terracotta.connection.ConnectionService;
import org.terracotta.consensus.entity.CoordinationServerEntityService;
import org.terracotta.consensus.entity.client.ClientCoordinationEntityService;
import org.terracotta.passthrough.PassthroughServer;

public class UnitTestConnectionService implements ConnectionService {

  public static PassthroughServer createServer() {
    PassthroughServer newServer = new PassthroughServer(true);
    newServer.registerServerEntityService(new EhcacheServerEntityService());
    newServer.registerClientEntityService(new EhcacheClientEntityService());
    newServer.registerServerEntityService(new CoordinationServerEntityService());
    newServer.registerClientEntityService(new ClientCoordinationEntityService());
    newServer.start();
    return newServer;
  }

  private static PassthroughServer server = createServer();

  public static synchronized void reset() {
    server.stop();
    server = createServer();
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
    return server().connectNewClient();
  }
}
