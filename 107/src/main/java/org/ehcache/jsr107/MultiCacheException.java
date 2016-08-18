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
package org.ehcache.jsr107;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;

import javax.cache.CacheException;

class MultiCacheException extends CacheException {
  private static final long serialVersionUID = -6839700789356356261L;

  private final List<Throwable> throwables = new ArrayList<Throwable>();

  MultiCacheException() {
    super();
  }

  MultiCacheException(Throwable t) {
    addThrowable(t);
  }

  void addThrowable(Throwable t) {
    if (t == null) {
      throw new NullPointerException();
    }

    if (t == this) {
      throw new IllegalArgumentException("cannot add to self");
    }

    if (t instanceof MultiCacheException) {
      for (Throwable t2 : ((MultiCacheException)t).getThrowables()) {
        throwables.add(t2);
      }
    } else {
      throwables.add(t);
    }
  }

  private List<Throwable> getThrowables() {
    return Collections.unmodifiableList(throwables);
  }

  @Override
  public String getMessage() {
    List<Throwable> ts = getThrowables();
    if (ts.isEmpty()) {
      return super.getMessage();
    } else {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < ts.size(); i++) {
        sb.append("[Exception ").append(i).append("] ").append(ts.get(i).getMessage()).append("\n");
      }
      return sb.deleteCharAt(sb.length() - 1).toString();
    }
  }

  MultiCacheException addFirstThrowable(Throwable t) {
    if (t == null) {
      throw new NullPointerException();
    }

    if (t == this) {
      throw new IllegalArgumentException("cannot add to self");
    }

    if (t instanceof MultiCacheException) {
      MultiCacheException mce = (MultiCacheException) t;
      throwables.addAll(0, mce.getThrowables());
    }
    throwables.add(0, t);
    return this;
  }

  @Override
  public Throwable initCause(Throwable cause) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Throwable getCause() {
    return null;
  }

  @Override
  public void printStackTrace() {
    super.printStackTrace();
    for (int i = 0; i < throwables.size(); i++) {
      System.err.print("  [Exception " + i + "] ");
      throwables.get(i).printStackTrace();
    }
  }

  @Override
  public void printStackTrace(PrintStream ps) {
    super.printStackTrace(ps);
    for (int i = 0; i < throwables.size(); i++) {
      ps.print("  [Exception " + i + "] ");
      throwables.get(i).printStackTrace(ps);
    }
  }

  @Override
  public void printStackTrace(PrintWriter pw) {
    super.printStackTrace(pw);
    for (int i = 0; i < throwables.size(); i++) {
      pw.print("  [Exception " + i + "] ");
      throwables.get(i).printStackTrace(pw);
    }
  }

  void throwIfNotEmpty() {
    if (!throwables.isEmpty()) {

      // if the only thing we contain is a single CacheException, then throw that
      if (throwables.size() == 1) {
        Throwable t = throwables.get(0);
        if (t instanceof CacheException) {
          throw (CacheException)t;
        }
      }

      throw this;
    }
  }
}
