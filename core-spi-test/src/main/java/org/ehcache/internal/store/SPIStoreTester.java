/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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

package org.ehcache.internal.store;

import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.spi.test.LegalSPITesterException;
import org.ehcache.spi.test.SPITester;

/**
 * Parent class for all Store tester classes.
 * It contains all common utility methods (e.g. instantiate a
 * {@link Store.ValueHolder Store.ValueHolder}).
 *
 * @author Aurelien Broszniowski
 */

public class SPIStoreTester<K, V> extends SPITester {

  protected static final String SPI_WARNING = "Warning, an exception is thrown due to the SPI test";

  protected final StoreFactory<K,V> factory;

  public SPIStoreTester(final StoreFactory<K,V> factory) {
    this.factory = factory;
  }

  @FunctionalInterface
  protected interface StoreRunnable {
    void run() throws StoreAccessException;
  }

  protected <T extends Throwable> T expectException(Class<T> expected, StoreRunnable action)
      throws LegalSPITesterException {
    try {
      action.run();
    } catch (Throwable throwable) {
      if (expected.isInstance(throwable)) {
        return expected.cast(throwable);
      }
      if (throwable instanceof StoreAccessException) {
        throw new LegalSPITesterException(SPI_WARNING, throwable);
      }
      if (throwable instanceof RuntimeException) {
        throw (RuntimeException) throwable;
      }
      if (throwable instanceof Error) {
        throw (Error) throwable;
      }
      throw new AssertionError("Unexpected checked exception", throwable);
    }
    throw new AssertionError("Expected " + expected.getSimpleName() + " to be thrown");
  }

}
