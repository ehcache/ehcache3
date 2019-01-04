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

package org.ehcache.transactions.xa.internal.journal;

import org.ehcache.spi.serialization.Serializer;
import org.ehcache.transactions.xa.internal.TransactionId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.ehcache.transactions.xa.internal.TypeUtil.uncheckedCast;

/**
 * A persistent, but not durable {@link Journal} implementation.
 * This implementation will persist saved states during close and restore them during open. If close is not called,
 * all saved states are lost.
 *
 * @author Ludovic Orban
 */
public class PersistentJournal<K> extends TransientJournal<K> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PersistentJournal.class);
  private static final String JOURNAL_FILENAME = "journal.data";

  protected static class SerializableEntry<K> implements Serializable {
    private static final long serialVersionUID = -6586025792671381923L;
    final XAState state;
    final boolean heuristic;
    final Collection<byte[]> serializedKeys;
    protected SerializableEntry(Entry<K> entry, Serializer<K> keySerializer) {
      this.state = entry.state;
      this.heuristic = entry.heuristic;
      this.serializedKeys = new ArrayList<>();
      for (K key : entry.keys) {
        ByteBuffer byteBuffer = keySerializer.serialize(key);
        byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
        this.serializedKeys.add(bytes);
      }
    }

    protected Collection<K> deserializeKeys(Serializer<K> keySerializer) throws ClassNotFoundException {
      Collection<K> result = new ArrayList<>();
      for (byte[] serializedKey : serializedKeys) {
        K key = keySerializer.read(ByteBuffer.wrap(serializedKey));
        result.add(key);
      }
      return result;
    }
  }


  private final File directory;
  private final Serializer<K> keySerializer;

  public PersistentJournal(File directory, Serializer<K> keySerializer) {
    if (directory == null) {
      throw new NullPointerException("directory must not be null");
    }
    if (keySerializer == null) {
      throw new NullPointerException("keySerializer must not be null");
    }
    this.directory = directory;
    this.keySerializer = keySerializer;
  }

  @Override
  public void open() throws IOException {
    File file = new File(directory, JOURNAL_FILENAME);
    if (file.isFile()) {
      try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file))) {
        boolean valid = ois.readBoolean();
        states.clear();
        if (valid) {
          Map<TransactionId, SerializableEntry<K>> readStates = uncheckedCast(ois.readObject());
          for (Map.Entry<TransactionId, SerializableEntry<K>> entry : readStates.entrySet()) {
            SerializableEntry<K> value = entry.getValue();
            states.put(entry.getKey(), new Entry<>(value.state, value.heuristic, value.deserializeKeys(keySerializer)));
          }
        }
      } catch (IOException ioe) {
        LOGGER.warn("Cannot read XA journal, truncating it", ioe);
      } catch (ClassNotFoundException cnfe) {
        LOGGER.warn("Cannot deserialize XA journal contents, truncating it", cnfe);
      } finally {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file))) {
          oos.writeObject(false);
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File(directory, JOURNAL_FILENAME)))) {
      oos.writeBoolean(true);
      Map<TransactionId, SerializableEntry<K>> toSerialize = new HashMap<>();
      for (Map.Entry<TransactionId, Entry<K>> entry : states.entrySet()) {
        TransactionId key = entry.getKey();
        Entry<K> value = entry.getValue();
        toSerialize.put(key, new SerializableEntry<>(value, keySerializer));
      }
      oos.writeObject(toSerialize);
      states.clear();
    }
  }
}
