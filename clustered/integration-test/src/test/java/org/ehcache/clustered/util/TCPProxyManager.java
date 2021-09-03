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
package org.ehcache.clustered.util;

import com.tc.net.proxy.TCPProxy;

import org.terracotta.utilities.test.net.PortManager;

import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;

/**
 * Manages creation and use of {@link TCPProxy} instances for a collection of Terracotta servers.
 */
public class TCPProxyManager implements AutoCloseable {
  private static final String STRIPE_SEPARATOR = ",";

  private final List<PortManager.PortRef> proxyPorts;
  private final List<TCPProxy> proxies;

  private TCPProxyManager(List<PortManager.PortRef> proxyPorts, List<TCPProxy> proxies) {
    this.proxyPorts = unmodifiableList(proxyPorts);
    this.proxies = unmodifiableList(proxies);
  }

  /**
   * Creates a new {@code TCPProxyManager} instance holding {@link TCPProxy} instances for
   * the endpoints represented in the specified connection URI.
   * <p>
   * This method creates {@code TCPProxy} instances for servers on {@code localhost}.
   * <p>
   * A reference to the returned {@code TCPProxyManager} must be maintained for the duration
   * of the use of the {@code TCPProxy} instances contained therein -- allowing the
   * {@code TCPProxyManager} to become <i>weakly-referenced</i> while the proxies are in
   * active use could result in random connection-related failures.
   *
   * @param connectionURI the {@code terracotta} connection URI
   * @return a new {@code TCPProxyManager} instance with a {@code TCPProxy} for each endpoint
   * @throws Exception if
   */
  public static TCPProxyManager create(URI connectionURI) throws Exception {
    List<Integer> primaryPorts = parsePorts(connectionURI);
    List<PortManager.PortRef> proxyPorts = PortManager.getInstance().reservePorts(primaryPorts.size());

    List<TCPProxy> proxies = new ArrayList<>(primaryPorts.size());
    InetAddress host = InetAddress.getByName("localhost");
    try {
      for (int i = 0; i < primaryPorts.size(); i++) {
        TCPProxy proxy = new TCPProxy(proxyPorts.get(i).port(), host, primaryPorts.get(i), 0L, false, null);
        proxies.add(proxy);
        proxy.start();
      }
    } catch (Exception e) {
      proxyPorts.forEach(PortManager.PortRef::close);
      throw e;
    }

    return new TCPProxyManager(proxyPorts, proxies);
  }

  /**
   * Returns the URI to use for the proxy connection to the Terracotta cluster.
   * @return the URI for connection to the Terracotta cluster via the allocated {@link TCPProxy} instances
   */
  public URI getURI() {
    String uri = proxyPorts.stream()
      .map(portRef -> "localhost:" + portRef.port())
      .collect(Collectors.joining(STRIPE_SEPARATOR, "terracotta://", ""));

    return URI.create(uri);
  }

  /**
   * Sets the delay for each allocated {@link TCPProxy} instance.
   * @param delay the non-negative delay
   */
  public void setDelay(long delay) {
    proxies.forEach(p -> p.setDelay(delay));
  }

  /**
   * Stops each allocated {@link TCPProxy} instance and releases the allocated proxy ports.
   */
  @Override
  public void close() {
    proxies.forEach(TCPProxy::stop);
    proxyPorts.forEach(PortManager.PortRef::close);
  }

  private static List<Integer> parsePorts(URI connectionURI) {
    String withoutProtocol = connectionURI.toString().substring(13);
    return Arrays.stream(withoutProtocol.split(STRIPE_SEPARATOR))
      .map(stripe -> stripe.substring(stripe.indexOf(":") + 1))
      .mapToInt(Integer::parseInt)
      .boxed()
      .collect(Collectors.toList());
  }
}
