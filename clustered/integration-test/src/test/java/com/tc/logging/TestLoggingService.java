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

package com.tc.logging;

import java.net.URI;

/**
 * A Terracotta logger writing to {@code System.out}.
 */
public class TestLoggingService implements TCLoggingService {

  @Override
  public TCLogger getLogger(final String name) {
    return new TCLogger() {

      private void log(String level, String message) {
        System.out.println(level + " [" + getName() + "] : " + message);
      }

      @Override
      public void debug(Object message) {
        log("DEBUG", message.toString());
      }

      @Override
      public void debug(Object message, Throwable t) {
        debug(message + " : " + t);
      }

      @Override
      public void error(Object message) {
        log("ERROR", message.toString());
      }

      @Override
      public void error(Object message, Throwable t) {
        error(message + " : " + t);
      }

      @Override
      public void fatal(Object message) {
        log("FATAL", message.toString());
      }

      @Override
      public void fatal(Object message, Throwable t) {
        fatal(message + " : " + t);
      }

      @Override
      public void info(Object message) {
        log("INFO", message.toString());
      }

      @Override
      public void info(Object message, Throwable t) {
        info(message + " : " + t);
      }

      @Override
      public void warn(Object message) {
        log("WARN", message.toString());
      }

      @Override
      public void warn(Object message, Throwable t) {
        warn(message + " : " + t);
      }

      @Override
      public boolean isDebugEnabled() {
        return true;
      }

      @Override
      public boolean isInfoEnabled() {
        return true;
      }

      @Override
      public void setLevel(LogLevel level) {
        //no-op
      }

      @Override
      public LogLevel getLevel() {
        return null;
      }

      @Override
      public String getName() {
        return name;
      }
    };
  }

  @Override
  public TCLogger getLogger(Class<?> c) {
    return getLogger(c.getName());
  }

  @Override
  public TCLogger getTestingLogger(String name) {
    return getLogger(name);
  }

  @Override
  public TCLogger getConsoleLogger() {
    return getLogger("console.logger");
  }

  @Override
  public TCLogger getOperatorEventLogger() {
    return getLogger("operator.event");
  }

  @Override
  public TCLogger getDumpLogger() {
    return getLogger("dump.logger");
  }

  @Override
  public TCLogger getCustomerLogger(String name) {
    return getLogger("customer.logger." + name);
  }

  @Override
  public void setLogLocationAndType(URI location, int processType) {
  }
}
