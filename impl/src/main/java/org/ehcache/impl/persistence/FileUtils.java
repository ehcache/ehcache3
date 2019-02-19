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

package org.ehcache.impl.persistence;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Stream;

import static java.lang.Integer.toHexString;
import static java.nio.charset.Charset.forName;
import static java.nio.file.Files.deleteIfExists;
import static java.nio.file.Files.isDirectory;
import static java.nio.file.Files.list;
import static java.util.stream.Collectors.toList;

/**
 * A bunch of utility functions, mainly used by {@link DefaultLocalPersistenceService} and
 * {@link FileBasedStateRepository} within this class.
 */
final class FileUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileUtils.class);
  private static final Charset UTF8 = forName("UTF8");
  private static final int DEL = 0x7F;
  private static final char ESCAPE = '%';

  private static final Set<Character> ILLEGALS = new HashSet<>();
  static {
    ILLEGALS.add('/');
    ILLEGALS.add('\\');
    ILLEGALS.add('<');
    ILLEGALS.add('>');
    ILLEGALS.add(':');
    ILLEGALS.add('"');
    ILLEGALS.add('|');
    ILLEGALS.add('?');
    ILLEGALS.add('*');
    ILLEGALS.add('.');
  }

  static String validateName(String name) {
    if (name.matches("[a-zA-Z0-9\\-_]+")) {
      return name;
    } else {
      throw new IllegalArgumentException("Name is invalid for persistence context: " + name);
    }
  }

  private static boolean recursiveDelete(Path file) {
    Deque<Path> toDelete = new ArrayDeque<>();
    toDelete.push(file);
    try {
      while (!toDelete.isEmpty()) {
        Path target = toDelete.pop();
        if (isDirectory(target)) {
          try (Stream<Path> list = list(target)) {
            List<Path> contents = list.collect(toList());
            if (contents.isEmpty()) {
              deleteIfExists(target);
            } else {
              toDelete.push(target);
              contents.forEach(toDelete::push);
            }
          }
        } else {
          deleteIfExists(target);
        }
      }
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  @SuppressFBWarnings("DM_GC")
  static boolean tryRecursiveDelete(Path file) {
    boolean interrupted = Thread.interrupted();
    try {
      for (int i = 0; i < 5; i++) {
        if (recursiveDelete(file) || !isWindows()) {
          return true;
        } else {
          System.gc();
          System.runFinalization();

          try {
            Thread.sleep(50);
          } catch (InterruptedException e) {
            interrupted = true;
          }
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
    return false;
  }

  private static boolean isWindows() {
    return System.getProperty("os.name").toLowerCase(Locale.ENGLISH).contains("windows");
  }

  /**
   * sanitize a name for valid file or directory name
   *
   * @param name the name to sanitize
   * @return sanitized version of name
   */
  static String safeIdentifier(String name) {
    return safeIdentifier(name, true);
  }

  static String safeIdentifier(String name, boolean withSha1) {
    int len = name.length();
    StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; i++) {
      char c = name.charAt(i);
      if (c <= ' ' || c >= DEL || ILLEGALS.contains(c) || c == ESCAPE) {
        sb.append(ESCAPE);
        sb.append(String.format("%04x", (int) c));
      } else {
        sb.append(c);
      }
    }
    if (withSha1) {
      sb.append("_").append(sha1(name));
    }
    return sb.toString();
  }

  private static String sha1(String input) {
    StringBuilder sb = new StringBuilder();
    for (byte b : getSha1Digest().digest(input.getBytes(UTF8))) {
      sb.append(toHexString((b & 0xf0) >>> 4));
      sb.append(toHexString((b & 0xf)));
    }
    return sb.toString();
  }

  private static MessageDigest getSha1Digest() {
    try {
      return MessageDigest.getInstance("SHA-1");
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError("All JDKs must have SHA-1");
    }
  }
}
