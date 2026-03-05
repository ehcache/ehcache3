/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2026
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
package org.ehcache.demos.server;

import jakarta.servlet.ServletContextListener;
import jakarta.servlet.http.HttpServlet;
import java.util.Objects;
import java.util.function.Supplier;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;

public final class EmbeddedPeeperServer {

  private EmbeddedPeeperServer() {
  }

  public static void run(Supplier<? extends ServletContextListener> listenerSupplier,
                         Supplier<? extends HttpServlet> servletSupplier) throws Exception {
    Objects.requireNonNull(listenerSupplier, "listenerSupplier");
    Objects.requireNonNull(servletSupplier, "servletSupplier");

    Server server = new Server();

    ServerConnector connector = new ServerConnector(server);
    connector.setPort(8080);
    connector.setHost(System.getenv().getOrDefault("HOST", "0.0.0.0"));
    server.addConnector(connector);

    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/");
    context.addEventListener(listenerSupplier.get());
    context.addServlet(new ServletHolder(servletSupplier.get()), "/*");

    server.setHandler(context);

    server.start();
    server.join();
  }
}
