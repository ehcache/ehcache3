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
package com.pany.ehcache.service;

import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceProvider;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class AccountingService implements Service {

  public enum Action {
    START, STOP
  }

  public static final List<Action> ACTIONS = new CopyOnWriteArrayList<>();

  public AccountingService() {
    System.getProperties(); // no-op
  }

  @Override
  public void start(ServiceProvider<Service> serviceProvider) {
    ACTIONS.add(Action.START);
  }

  @Override
  public void stop() {
    ACTIONS.add(Action.STOP);
  }

}
