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

package org.ehcache.impl.internal.util;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.io.File;
import java.io.FilenameFilter;

/**
 * Matcher(s) for file existence in the persistence directory..
 *
 * @author RKAV
 */
public class FileExistenceMatchers {
  public static Matcher<File> fileExistsOwnerOpen(final int numExpectedFiles) {
    return new TypeSafeMatcher<File>() {
      @Override
      protected boolean matchesSafely(File item) {
        return fileExistsOwnerOpenWithName(item, numExpectedFiles, null);
      }

      @Override
      public void describeTo(Description description) {

      }
    };
  }

  public static Matcher<File> fileExistsOwnerOpenExpected(final int numExpectedFiles, final String expected) {
    return new TypeSafeMatcher<File>() {
      @Override
      protected boolean matchesSafely(File item) {
        return fileExistsOwnerOpenWithName(item, numExpectedFiles, expected);
      }

      @Override
      public void describeTo(Description description) {
      }
    };
  }

  public static Matcher<File> fileExistOwnerClosed(final int numExpectedFiles) {
    return new TypeSafeMatcher<File>() {
      @Override
      protected boolean matchesSafely(File item) {
        return fileExistsOwnerClosedWithName(item, numExpectedFiles, null);
      }

      @Override
      public void describeTo(Description description) {
      }
    };
  }

  public static Matcher<File> fileExistOwnerClosedExpected(final int numExpectedFiles, final String expected) {
    return new TypeSafeMatcher<File>() {
      @Override
      protected boolean matchesSafely(File item) {
        return fileExistsOwnerClosedWithName(item, numExpectedFiles, expected);
      }

      @Override
      public void describeTo(Description description) {
      }
    };
  }

  public static Matcher<File> fileExistNoOwner() {
    return new TypeSafeMatcher<File>() {
      @Override
      protected boolean matchesSafely(File item) {
        File[] files = item.listFiles();
        return files == null || files.length == 0;
      }

      @Override
      public void describeTo(Description description) {

      }
    };
  }

  private static boolean fileExistsOwnerOpenWithName(final File item, final int numExpectedFiles, final String expected) {
    boolean matches = false;
    File[] files = item.listFiles();
    if (files == null) {
      return false;
    }
    if (files.length == 2) {
      int i = files[0].isDirectory() ? 0 : 1;
      if (expected != null) {
        files = files[i].listFiles(new FilenameFilter() {
          @Override
          public boolean accept(File dir, String name) {
            return name.startsWith(expected);
          }
        });
      } else {
        files = files[i].isDirectory() ? files[i].listFiles() : null;
      }
      if (numExpectedFiles > 0) {
        matches = files != null && files.length == numExpectedFiles;
      } else {
        matches = files == null || files.length == 0;
      }
    }
    return matches;
  }

  private static boolean fileExistsOwnerClosedWithName(final File item, final int numExpectedFiles, final String expected) {
    boolean matches = false;
    File[] files = item.listFiles();
    if (files == null) {
      return false;
    }
    if (files.length == 1) {
      if (expected != null) {
        files = files[0].listFiles(new FilenameFilter() {
          @Override
          public boolean accept(File dir, String name) {
            return name.startsWith(expected);
          }
        });
      } else {
        files = files[0].isDirectory() ? files[0].listFiles() : null;
      }
      if (numExpectedFiles > 0) {
        matches = files != null && files.length == numExpectedFiles;
      } else {
        matches = files == null || files.length == 0;
      }
    }
    return matches;
  }
}
