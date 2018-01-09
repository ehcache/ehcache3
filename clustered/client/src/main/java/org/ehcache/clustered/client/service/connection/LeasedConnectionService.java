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

package org.ehcache.clustered.client.service.connection;

import com.terracotta.connection.api.TerracottaConnectionService;
import org.ehcache.clustered.client.config.Timeouts;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionException;
import org.terracotta.connection.ConnectionPropertyNames;
import org.terracotta.connection.ConnectionService;
import org.terracotta.lease.connection.BasicLeasedConnection;
import org.terracotta.lease.connection.TimeBudget;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class LeasedConnectionService implements ConnectionService {

  private static final TerracottaConnectionService CONNECTION_SERVICE = new TerracottaConnectionService();

  @Override
  public boolean handlesURI(URI uri) {
    return CONNECTION_SERVICE.handlesURI(uri);
  }

  @Override
  public Connection connect(URI uri, Properties properties) throws ConnectionException {

    Connection connection = CONNECTION_SERVICE.connect(uri, properties);

    BasicLeasedConnection basicLeasedConnection = BasicLeasedConnection.create(connection, createTimeBudget(properties));

    return basicLeasedConnection;
  }

  private TimeBudget createTimeBudget(Properties properties) {
    String timeoutString = properties.getProperty(ConnectionPropertyNames.CONNECTION_TIMEOUT,
            Long.toString(Timeouts.DEFAULT_CONNECTION_TIMEOUT.toMillis()));
    long timeout = Long.parseLong(timeoutString);
    return new TimeBudget(timeout, TimeUnit.MILLISECONDS);
  }
}
