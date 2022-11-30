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
import org.ehcache.impl.serialization.TransientStateHolder;
import org.ehcache.spi.persistence.StateHolder;
import org.ehcache.spi.persistence.StateRepository;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static org.ehcache.impl.persistence.FileUtils.safeIdentifier;

/**
 * FileBasedStateRepository
 */
class FileBasedStateRepository implements StateRepository, Closeable {

  private static final String HOLDER_FILE_PREFIX = "holder-";
  private static final String HOLDER_FILE_SUFFIX = ".bin";
  private final File dataDirectory;
  private final ConcurrentMap<String, Tuple> knownHolders;
  private final AtomicInteger nextIndex = new AtomicInteger();

  FileBasedStateRepository(File directory) throws CachePersistenceException {
    if (directory == null) {
      throw new NullPointerException("directory must be non null");
    }
    if (!directory.isDirectory()) {
      throw new IllegalArgumentException(directory + " is not a directory");
    }
    this.dataDirectory = directory;
    knownHolders = new ConcurrentHashMap<>();
    loadMaps();
  }

  @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
  private void loadMaps() throws CachePersistenceException {
    try {
      //noinspection ConstantConditions
      for (File file : dataDirectory.listFiles((dir, name) -> name.endsWith(HOLDER_FILE_SUFFIX))) {
        try (FileInputStream fis = new FileInputStream(file);
             ObjectInputStream oin = new ObjectInputStream(fis)) {
          String name = (String) oin.readObject();
          Tuple tuple = (Tuple) oin.readObject();
          if (nextIndex.get() <= tuple.index) {
            nextIndex.set(tuple.index + 1);
          }
          knownHolders.put(name, tuple);
        }
      }
    } catch (Exception e) {
      knownHolders.clear();
      throw new CachePersistenceException("Failed to load existing StateRepository data", e);
    }
  }

  private void saveMaps() throws IOException {
    for (Map.Entry<String, Tuple> entry : knownHolders.entrySet()) {
      File outFile = new File(dataDirectory, createFileName(entry));
      try (FileOutputStream fos = new FileOutputStream(outFile);
           ObjectOutputStream oos = new ObjectOutputStream(fos)) {
        oos.writeObject(entry.getKey());
        oos.writeObject(entry.getValue());
      }
    }
  }

  private String createFileName(Map.Entry<String, Tuple> entry) {return HOLDER_FILE_PREFIX + entry.getValue().index + "-" + safeIdentifier(entry.getKey(), false) + HOLDER_FILE_SUFFIX;}

  @Override
  public <K extends Serializable, V extends Serializable> StateHolder<K, V> getPersistentStateHolder(String name,
                                                                                                     Class<K> keyClass,
                                                                                                     Class<V> valueClass,
                                                                                                     Predicate<Class<?>> isClassPermitted,
                                                                                                     ClassLoader classLoader) {
    // isClassPermitted and  classLoader are ignored because this state repository has already being read from file and cached in
    Tuple result = knownHolders.get(name);
    if (result == null) {
      StateHolder<K, V> holder = new TransientStateHolder<>();
      result = knownHolders.putIfAbsent(name, new Tuple(nextIndex.getAndIncrement(), holder));

      if (result == null) {
        return holder;
      }
    }

    @SuppressWarnings("unchecked")
    StateHolder<K, V> holder = (StateHolder<K, V>) result.holder;
    return holder;
  }

  @Override
  public void close() throws IOException {
    saveMaps();
  }

  static class Tuple implements Serializable {

    private static final long serialVersionUID = 664492058736170101L;

    final int index;
    final StateHolder<?, ?> holder;

    Tuple(int index, StateHolder<?, ?> holder) {
      this.index = index;
      this.holder = holder;
    }
  }
}
