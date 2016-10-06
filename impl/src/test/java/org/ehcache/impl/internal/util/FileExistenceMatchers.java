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
 * Matchers for file locks and existence in the persistence directory.
 *
 * @author RKAV
 */
public class FileExistenceMatchers {

  private static class DirectoryIsLockedMatcher extends TypeSafeMatcher<File> {
    @Override
    protected boolean matchesSafely(File dir) {
      File[] files = dir.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.equals(".lock");
        }
      });
      return files != null && files.length == 1;
    }

    @Override
    public void describeMismatchSafely(File item, Description mismatchDescription) {
      mismatchDescription.appendValue(item)
        .appendText(" doesn't contain a .lock file");
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("a .lock file in the directory");
    }
  }

  private static class ContainsCacheDirectoryMatcher extends TypeSafeMatcher<File> {

    private String parentDirectory;
    private String startWith;

    public ContainsCacheDirectoryMatcher(String safeSpaceOwner, String cacheAlias) {
      this.parentDirectory = safeSpaceOwner;
      this.startWith = cacheAlias + "_";
    }

    @Override
    protected boolean matchesSafely(File item) {
      // The directory layout is that there will be a directory named 'file'
      // If the cache directory exists, it will contain a directory starting with 'cacheAlias_'

      File file = new File(item, parentDirectory);
      if(!file.exists() || !file.isAbsolute()) {
        return false;
      }

      File[] files = file.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.startsWith(startWith);
        }
      });

      return files != null && files.length == 1 && files[0].isDirectory();
    }

    @Override
    public void describeMismatchSafely(File item, Description mismatchDescription) {
      mismatchDescription.appendValue(item)
        .appendText(" doesn't contains a file starting with " + startWith);
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("contains a file starting with '" + "'");
    }
  }

  /**
   * Matcher checking if the persistence directory is locked by a cache manager
   *
   * @return the matcher
   */
  public static Matcher<File> isLocked() {
    return new DirectoryIsLockedMatcher();
  }

  /**
   * Matcher checking if a cache directory starting with this name exists in the 'file' safe space
   *
   * @param cacheAlias cache alias that will be the prefix of the cache directory
   * @return the matcher
   */
  public static Matcher<File> containsCacheDirectory(String cacheAlias) {
    return new ContainsCacheDirectoryMatcher("file", cacheAlias);
  }

  /**
   * Matcher checking if a cache directory starting within the safe space
   *
   * @param safeSpaceOwner name of the same space owner. It is also the name of the safe space root directory
   * @param cacheAlias cache alias that will be the prefix of the cache directory
   * @return the matcher
   */
  public static Matcher<File> containsCacheDirectory(String safeSpaceOwner, String cacheAlias) {
    return new ContainsCacheDirectoryMatcher(safeSpaceOwner, cacheAlias);
  }

}
