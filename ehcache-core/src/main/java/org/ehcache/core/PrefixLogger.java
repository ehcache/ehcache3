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

package org.ehcache.core;

import org.slf4j.Logger;
import org.slf4j.Marker;

/**
 * This class represents a decorated {@code org.slf4j.Logger}
 * which will log the information along with context key-value pair
 */
class PrefixLogger implements Logger {

  private final Logger delegate;
  private final String prefix;
  private final String formatEscapedPrefix;

  public PrefixLogger(Logger logger, String prefix) {
    this.delegate = logger;
    this.prefix = prefix;
    this.formatEscapedPrefix = formatEscape(prefix);
  }

  private String formatEscape(String string) {
    return string
      .replace("\\{}", "\\'{''}'")
      .replace("{}", "\\{}");
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public boolean isTraceEnabled() {
    return delegate.isTraceEnabled();
  }

  @Override
  public void trace(String msg) {
    delegate.trace(prefix + msg);
  }

  @Override
  public void trace(String format, Object arg) {
    delegate.trace(formatEscapedPrefix + format, arg);
  }

  @Override
  public void trace(String format, Object arg1, Object arg2) {
    delegate.trace(formatEscapedPrefix + format, arg1, arg2);
  }

  @Override
  public void trace(String format, Object... arguments) {
    delegate.trace(formatEscapedPrefix + format, arguments);
  }

  @Override
  public void trace(String msg, Throwable t) {
    delegate.trace(prefix + msg, t);
  }

  @Override
  public boolean isTraceEnabled(Marker marker) {
    return delegate.isTraceEnabled(marker);
  }

  @Override
  public void trace(Marker marker, String msg) {
    delegate.trace(marker, prefix + msg);
  }

  @Override
  public void trace(Marker marker, String format, Object arg) {
    delegate.trace(marker, formatEscapedPrefix + format, arg);
  }

  @Override
  public void trace(Marker marker, String format, Object arg1, Object arg2) {
    delegate.trace(marker, formatEscapedPrefix + format, arg1, arg2);
  }

  @Override
  public void trace(Marker marker, String format, Object... argArray) {
    delegate.trace(marker, formatEscapedPrefix + format, argArray);
  }

  @Override
  public void trace(Marker marker, String msg, Throwable t) {
    delegate.trace(marker, prefix + msg, t);
  }

  @Override
  public boolean isDebugEnabled() {
    return delegate.isDebugEnabled();
  }

  @Override
  public void debug(String msg) {
    delegate.debug(prefix + msg);
  }

  @Override
  public void debug(String format, Object arg) {
    delegate.debug(formatEscapedPrefix + format, arg);
  }

  @Override
  public void debug(String format, Object arg1, Object arg2) {
    delegate.debug(formatEscapedPrefix + format, arg1, arg2);
  }

  @Override
  public void debug(String format, Object... arguments) {
    delegate.debug(formatEscapedPrefix + format, arguments);
  }

  @Override
  public void debug(String msg, Throwable t) {
    delegate.debug(prefix + msg, t);
  }

  @Override
  public boolean isDebugEnabled(Marker marker) {
    return delegate.isDebugEnabled(marker);
  }

  @Override
  public void debug(Marker marker, String msg) {
    delegate.debug(marker, prefix + msg);
  }

  @Override
  public void debug(Marker marker, String format, Object arg) {
    delegate.debug(marker, formatEscapedPrefix + format, arg);
  }

  @Override
  public void debug(Marker marker, String format, Object arg1, Object arg2) {
    delegate.debug(marker, formatEscapedPrefix + format, arg1, arg2);
  }

  @Override
  public void debug(Marker marker, String format, Object... arguments) {
    delegate.debug(marker, formatEscapedPrefix + format, arguments);
  }

  @Override
  public void debug(Marker marker, String msg, Throwable t) {
    delegate.debug(marker, prefix + msg, t);
  }

  @Override
  public boolean isInfoEnabled() {
    return delegate.isInfoEnabled();
  }

  @Override
  public void info(String msg) {
    delegate.info(prefix + msg);
  }

  @Override
  public void info(String format, Object arg) {
    delegate.info(formatEscapedPrefix + format, arg);
  }

  @Override
  public void info(String format, Object arg1, Object arg2) {
    delegate.info(formatEscapedPrefix + format, arg1, arg2);
  }

  @Override
  public void info(String format, Object... arguments) {
    delegate.info(formatEscapedPrefix + format, arguments);
  }

  @Override
  public void info(String msg, Throwable t) {
    delegate.info(prefix + msg, t);
  }

  @Override
  public boolean isInfoEnabled(Marker marker) {
    return delegate.isInfoEnabled(marker);
  }

  @Override
  public void info(Marker marker, String msg) {
    delegate.info(marker, prefix + msg);
  }

  @Override
  public void info(Marker marker, String format, Object arg) {
    delegate.info(marker, formatEscapedPrefix + format, arg);
  }

  @Override
  public void info(Marker marker, String format, Object arg1, Object arg2) {
    delegate.info(marker, formatEscapedPrefix + format, arg1, arg2);
  }

  @Override
  public void info(Marker marker, String format, Object... arguments) {
    delegate.info(marker, formatEscapedPrefix + format, arguments);
  }

  @Override
  public void info(Marker marker, String msg, Throwable t) {
    delegate.info(marker, prefix + msg, t);
  }

  @Override
  public boolean isWarnEnabled() {
    return delegate.isWarnEnabled();
  }

  @Override
  public void warn(String msg) {
    delegate.warn(prefix + msg);
  }

  @Override
  public void warn(String format, Object arg) {
    delegate.warn(formatEscapedPrefix + format, arg);
  }

  @Override
  public void warn(String format, Object... arguments) {
    delegate.warn(formatEscapedPrefix + format, arguments);
  }

  @Override
  public void warn(String format, Object arg1, Object arg2) {
    delegate.warn(formatEscapedPrefix + format, arg1, arg2);
  }

  @Override
  public void warn(String msg, Throwable t) {
    delegate.warn(prefix + msg, t);
  }

  @Override
  public boolean isWarnEnabled(Marker marker) {
    return delegate.isWarnEnabled(marker);
  }

  @Override
  public void warn(Marker marker, String msg) {
    delegate.warn(marker, prefix + msg);
  }

  @Override
  public void warn(Marker marker, String format, Object arg) {
    delegate.warn(marker, formatEscapedPrefix + format, arg);
  }

  @Override
  public void warn(Marker marker, String format, Object arg1, Object arg2) {
    delegate.warn(marker, formatEscapedPrefix + format, arg1, arg2);
  }

  @Override
  public void warn(Marker marker, String format, Object... arguments) {
    delegate.warn(marker, formatEscapedPrefix + format, arguments);
  }

  @Override
  public void warn(Marker marker, String msg, Throwable t) {
    delegate.warn(marker, prefix + msg, t);
  }

  @Override
  public boolean isErrorEnabled() {
    return delegate.isErrorEnabled();
  }

  @Override
  public void error(String msg) {
    delegate.error(prefix + msg);
  }

  @Override
  public void error(String format, Object arg) {
    delegate.error(formatEscapedPrefix + format, arg);
  }

  @Override
  public void error(String format, Object arg1, Object arg2) {
    delegate.error(formatEscapedPrefix + format, arg1, arg2);
  }

  @Override
  public void error(String format, Object... arguments) {
    delegate.error(formatEscapedPrefix + format, arguments);
  }

  @Override
  public void error(String msg, Throwable t) {
    delegate.error(prefix + msg, t);
  }

  @Override
  public boolean isErrorEnabled(Marker marker) {
    return delegate.isErrorEnabled(marker);
  }

  @Override
  public void error(Marker marker, String msg) {
    delegate.error(marker, prefix + msg);
  }

  @Override
  public void error(Marker marker, String format, Object arg) {
    delegate.error(marker, formatEscapedPrefix + format, arg);
  }

  @Override
  public void error(Marker marker, String format, Object arg1, Object arg2) {
    delegate.error(marker, formatEscapedPrefix + format, arg1, arg2);
  }

  @Override
  public void error(Marker marker, String format, Object... arguments) {
    delegate.error(marker, formatEscapedPrefix + format, arguments);
  }

  @Override
  public void error(Marker marker, String msg, Throwable t) {
    delegate.error(marker, prefix + msg, t);
  }
}
