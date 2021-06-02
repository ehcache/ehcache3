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

import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionException;
import org.terracotta.connection.ConnectionService;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Properties;

/**
 * MockConnectionService
 */
public class MockConnectionService implements ConnectionService {

  private static final String CONNECTION_TYPE = "mock";
  public static Connection mockConnection;

  @Override
  public boolean handlesURI(URI uri) {
    return handlesConnectionType(uri.getScheme());
  }

  @Override
  public boolean handlesConnectionType(String s) {
    return CONNECTION_TYPE.equals(s);
  }

  @Override
  public Connection connect(URI uri, Properties properties) throws ConnectionException {
    return getConnection();
  }

  @Override
  public Connection connect(Iterable<InetSocketAddress> iterable, Properties properties) throws ConnectionException {
    return getConnection();
  }

  private Connection getConnection() throws ConnectionException {
    if (mockConnection == null) {
      throw new ConnectionException(new IllegalStateException("Set mock connection first"));
    }
    return mockConnection;
  }
}
