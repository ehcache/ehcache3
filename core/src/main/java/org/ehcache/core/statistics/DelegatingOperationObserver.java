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
package org.ehcache.core.statistics;

public class DelegatingOperationObserver<T extends Enum<T>> implements OperationObserver<T> {

  private final org.terracotta.statistics.observer.OperationObserver<T> observer;

  public DelegatingOperationObserver(org.terracotta.statistics.observer.OperationObserver<T> operationObserver) {
    this.observer = operationObserver;
  }

  @Override
  public void begin() {
    this.observer.begin();
  }

  @Override
  public void end(T result) {
    this.observer.end(result);
  }
}
