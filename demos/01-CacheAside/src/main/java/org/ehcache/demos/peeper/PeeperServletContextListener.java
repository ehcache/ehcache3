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
package org.ehcache.demos.peeper;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

/**
 * @author Ludovic Orban
 */
@WebListener
public class PeeperServletContextListener implements ServletContextListener {

  public static final DataStore DATA_STORE = new DataStore();

  @Override
  public void contextInitialized(ServletContextEvent servletContextEvent) {
    try {
      DATA_STORE.init();
    } catch (Exception e) {
      throw new RuntimeException("Cannot initialize datastore", e);
    }
  }

  @Override
  public void contextDestroyed(ServletContextEvent servletContextEvent) {
    try {
      DATA_STORE.close();
    } catch (Exception e) {
        throw new RuntimeException("Cannot close datastore", e);
    }
  }
}
