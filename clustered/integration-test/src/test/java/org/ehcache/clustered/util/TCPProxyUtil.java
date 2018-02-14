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
import org.terracotta.testing.common.PortChooser;

import java.net.InetAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class TCPProxyUtil {

  private static final String STRIPE_SEPARATOR = ",";

  private TCPProxyUtil() {

  }

  public static URI getProxyURI(URI connectionURI, List<TCPProxy> proxies) throws Exception {

    List<Integer> ports = parsePorts(connectionURI);
    List<Integer> proxyPorts = createProxyPorts(ports.size());

    for (int i = 0; i < ports.size(); i++) {
      int port = ports.get(i);
      int proxyPort = proxyPorts.get(i);

      InetAddress host = InetAddress.getByName("localhost");
      TCPProxy proxy = new TCPProxy(proxyPort, host, port, 0L, false, null);
      proxies.add(proxy);
      proxy.start();
    }

    return createURI(proxyPorts);
  }

  private static List<Integer> parsePorts(URI connectionURI) {
    String uriString = connectionURI.toString();
    String withoutProtocol = uriString.substring(13);
    String[] stripes = withoutProtocol.split(STRIPE_SEPARATOR);

    return Arrays.stream(stripes)
            .map(stripe -> stripe.substring(stripe.indexOf(":") + 1))
            .mapToInt(Integer::parseInt)
            .boxed()
            .collect(Collectors.toList());
  }

  private static List<Integer> createProxyPorts(int portCount) {
    PortChooser portChooser = new PortChooser();
    int firstProxyPort = portChooser.chooseRandomPorts(portCount);

    return IntStream
            .range(0, portCount)
            .map(i -> firstProxyPort + i)
            .boxed()
            .collect(Collectors.toList());
  }

  private static URI createURI(List<Integer> proxyPorts) {

    String uri = proxyPorts.stream()
            .map(port -> "localhost:" + port)
            .collect(Collectors.joining(",", "terracotta://", ""));

    return URI.create(uri);
  }

  public static void setDelay(long delay, List<TCPProxy> proxies) {
    for (TCPProxy proxy : proxies) {
      proxy.setDelay(delay);
    }
  }
}
