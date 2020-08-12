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

import org.ehcache.CachePersistenceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import static java.lang.Integer.toHexString;
import static java.nio.charset.Charset.forName;

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

  static void createLocationIfRequiredAndVerify(final File rootDirectory) {
    if(!rootDirectory.exists()) {
      if(!rootDirectory.mkdirs()) {
        throw new IllegalArgumentException("Directory couldn't be created: " + rootDirectory.getAbsolutePath());
      }
    } else if(!rootDirectory.isDirectory()) {
      throw new IllegalArgumentException("Location is not a directory: " + rootDirectory.getAbsolutePath());
    }

    if(!rootDirectory.canWrite()) {
      throw new IllegalArgumentException("Location isn't writable: " + rootDirectory.getAbsolutePath());
    }
  }

  static File createSubDirectory(File mainDirectory, String name) throws CachePersistenceException {
    validateName(name);
    File subDirectory = new File(mainDirectory, name);
    create(subDirectory);
    return subDirectory;
  }

  static void validateName(String name) {
    if (!name.matches("[a-zA-Z0-9\\-_]+")) {
      throw new IllegalArgumentException("Name is invalid for persistence context: " + name);
    }
  }

  static void create(File directory) throws CachePersistenceException {
    if (directory.isDirectory()) {
      LOGGER.debug("Reusing {}", directory.getAbsolutePath());
    } else if (directory.mkdir()) {
      LOGGER.debug("Created {}", directory.getAbsolutePath());
    } else if (directory.isDirectory()) {
      // if create directory fails, check once more if it is due to concurrent creation.
      LOGGER.debug("Reusing {}", directory.getAbsolutePath());
    } else {
      throw new CachePersistenceException("Unable to create or reuse directory: " + directory.getAbsolutePath());
    }
  }

  static boolean recursiveDeleteDirectoryContent(File file) {
    File[] contents = file.listFiles();
    if (contents == null) {
      throw new IllegalArgumentException("File " + file.getAbsolutePath() + " is not a directory");
    } else {
      boolean deleteSuccessful = true;
      for (File f : contents) {
        deleteSuccessful &= tryRecursiveDelete(f);
      }
      return deleteSuccessful;
    }
  }

  private static boolean recursiveDelete(File file) {
    Deque<File> toDelete = new ArrayDeque<>();
    toDelete.push(file);
    while (!toDelete.isEmpty()) {
      File target = toDelete.pop();
      File[] contents = target.listFiles();
      if (contents == null || contents.length == 0) {
        if (target.exists() && !target.delete()) {
          return false;
        }
      } else {
        toDelete.push(target);
        for (File f : contents) {
          toDelete.push(f);
        }
      }
    }
    return true;
  }

  @SuppressFBWarnings("DM_GC")
  static boolean tryRecursiveDelete(File file) {
    boolean interrupted = false;
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
