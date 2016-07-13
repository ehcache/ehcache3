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
import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.ehcache.spi.persistence.StateRepository;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.ehcache.impl.persistence.DefaultLocalPersistenceService.safeIdentifier;

/**
 * FileBasedStateRepository
 */
class FileBasedStateRepository implements StateRepository, Closeable {

  private static final String MAP_FILE_PREFIX = "map-";
  private static final String MAP_FILE_SUFFIX = ".bin";
  private final File dataDirectory;
  private final ConcurrentMap<String, Tuple> knownMaps;
  private final AtomicInteger nextIndex = new AtomicInteger();

  FileBasedStateRepository(File directory) throws CachePersistenceException {
    if (directory == null) {
      throw new NullPointerException("directory must be non null");
    }
    if (!directory.isDirectory()) {
      throw new IllegalArgumentException(directory + " is not a directory");
    }
    this.dataDirectory = directory;
    knownMaps = new ConcurrentHashMap<String, Tuple>();
    loadMaps();
  }

  @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
  private void loadMaps() throws CachePersistenceException {
    try {
      for (File file : dataDirectory.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.endsWith(MAP_FILE_SUFFIX);
        }
      })) {
        FileInputStream fis = new FileInputStream(file);
        try {
          ObjectInputStream oin = new ObjectInputStream(fis);
          try {
            String name = (String) oin.readObject();
            Tuple tuple = (Tuple) oin.readObject();
            if (nextIndex.get() <= tuple.index) {
              nextIndex.set(tuple.index + 1);
            }
            knownMaps.put(name, tuple);
          } finally {
            oin.close();
          }
        } finally {
          fis.close();
        }
      }
    } catch (Exception e) {
      knownMaps.clear();
      throw new CachePersistenceException("Failed to load existing StateRepository data", e);
    }
  }

  private void saveMaps() throws IOException {
    for (Map.Entry<String, Tuple> entry : knownMaps.entrySet()) {
      File outFile = new File(dataDirectory, createFileName(entry));
      FileOutputStream fos = new FileOutputStream(outFile);
      try {
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        try {
          oos.writeObject(entry.getKey());
          oos.writeObject(entry.getValue());
        } finally {
          oos.close();
        }
      } finally {
        fos.close();
      }
    }
  }

  private String createFileName(Map.Entry<String, Tuple> entry) {return MAP_FILE_PREFIX + entry.getValue().index + "-" + safeIdentifier(entry.getKey(), false) + MAP_FILE_SUFFIX;}

  @Override
  public <K extends Serializable, V extends Serializable> ConcurrentMap<K, V> getPersistentConcurrentMap(String name, Class<K> keyClass, Class<V> valueClass) {
    Tuple result = knownMaps.get(name);
    if (result == null) {
      ConcurrentHashMap<K, V> newMap = new ConcurrentHashMap<K, V>();
      result = knownMaps.putIfAbsent(name, new Tuple(nextIndex.getAndIncrement(), newMap));

      if (result == null) {
        return newMap;
      }
    }
    return (ConcurrentMap<K, V>) result.map;
  }

  @Override
  public void close() throws IOException {
    saveMaps();
  }

  static class Tuple implements Serializable {
    final int index;
    final ConcurrentMap<?, ?> map;

    Tuple(int index, ConcurrentMap<?, ?> map) {
      this.index = index;
      this.map = map;
    }
  }
}
