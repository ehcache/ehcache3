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
import org.terracotta.utilities.io.Files;

import java.io.File;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import static java.lang.Integer.toHexString;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A bunch of utility functions, mainly used by {@link DefaultLocalPersistenceService} and
 * {@link FileBasedStateRepository} within this class.
 */
final class FileUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileUtils.class);
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

  static boolean tryRecursiveDelete(File file) {
    try {
      Files.deleteTree(file.toPath(), Duration.ofMillis(250), FileUtils::gc);
      return true;
    } catch (IOException ioe) {
      return false;
    }
  }

  @SuppressFBWarnings("DM_GC")
  private static void gc() {
    System.gc();
    System.runFinalization();
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
    for (byte b : getSha1Digest().digest(input.getBytes(UTF_8))) {
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
