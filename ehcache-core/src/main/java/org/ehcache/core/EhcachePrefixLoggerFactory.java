/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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
package org.ehcache.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Factory to create instances of a decorated {@code org.slf4j.Logger}
 * which will log the information along with context key-value pair
 *
 *
 * <p>
 * A thread can call {@link #withContext(String, String)} method to set context key-value pair
 *
 * <pre>
 * {@code
 * try(EhcachePrefixLoggerFactory.Context ignored = EhcachePrefixLoggerFactory.withContext("context-key", "context-value")){
 *
 * }
 *  }</pre>
 *
 *
 * <p>
 * A user can get an instances of {@code org.slf4j.Logger} by calling {@link #getLogger(Class)}
 * or {@link #getLogger(String)} method to log the context info
 *
 * <pre>
 *   {@code
 *     Logger logger = EhcachePrefixLoggerFactory.getLogger(A.class);
 *     Logger logger = EhcachePrefixLoggerFactory.getLogger("StringKey");
 *
 *  }
 *</pre>
 *
 */
public class EhcachePrefixLoggerFactory {

  private EhcachePrefixLoggerFactory() {
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
  }

  private static final ThreadLocal<Map<String, String>> bindTimeMdc = new ThreadLocal<>();

  /**
   * Exposing internal state for testing purposes
   */
  protected static Map<String, String> getContextMap() {
    return bindTimeMdc.get();
  }

  /**
   * Bind a new key-value logging context pair to the calling thread.
   *
   * Calls to {@link #getLogger(Class)} made from this thread, before the returned context is closed will see this key-value pair.
   *
   * @param key the context key
   * @param value the context value
   * @return an auto-closable context object that will remove the created binding.
   */
  public static Context withContext(String key, String value) {

    Map<String, String> context = getOrCreateContextMap();

    Thread creator = Thread.currentThread();
    String old = context.put(key, value);
    if (old == null) {
      return () -> {
        if (creator == Thread.currentThread()) {
          if (context.remove(key, value) && context.isEmpty()) {
            bindTimeMdc.remove();
          }
        } else {
          throw new UnsupportedOperationException("Context can only be closed by the creator thread");
        }
      };
    } else {
      return () -> context.replace(key, value, old);
    }
  }

  private static Map<String, String> getOrCreateContextMap() {
    Map<String, String> context = bindTimeMdc.get();
    if (context == null) {
      bindTimeMdc.set(context = new HashMap<>());
    }
    return context;
  }


  /**
   * This method allows a user to get a decorated {@code org.slf4j.Logger}
   * which will log the information along with context key-value pair
   *
   * @param klazz the class name
   * @return decorated logger
   */
  public static Logger getLogger(Class<?> klazz) {
    return wrappedLogger(klazz, LoggerFactory::getLogger);
  }

  /**
   * This method allows a user to get a decorated {@code org.slf4j.Logger}
   * which will log the information along with context key-value pair
   *
   * @param name the logger name
   * @return decorated logger
   */
  public static Logger getLogger(String name) {
    return wrappedLogger(name, LoggerFactory::getLogger);
  }

  private static <K> Logger wrappedLogger(K key, Function<K, Logger> loggerGenerator) {
    Map<String, String> context = bindTimeMdc.get();

    Logger logger = loggerGenerator.apply(key);

    if (context == null || context.isEmpty()) {
      return logger;
    } else {
      String prefix = context.toString();
      return new PrefixLogger(logger, prefix);
    }

  }

  @FunctionalInterface
  public interface Context extends AutoCloseable {

    @Override
    void close();
  }

}
