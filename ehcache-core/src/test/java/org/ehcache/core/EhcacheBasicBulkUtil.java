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

package org.ehcache.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

/**
 * Utility methods and common data for {@link Ehcache}
 * bulk method unit tests.
 *
 * @author Clifford W. Johnson
 */
final class EhcacheBasicBulkUtil {

  /** Private, niladic constructor to prevent instantiation. */
  private EhcacheBasicBulkUtil() {
  }

  /**
   * Entries for populating {@link Store Store} and
   * {@link CacheLoaderWriter} instances in the
   * unit tests.  The entries used are generally subset using the key sets
   * {@link #KEY_SET_A}, {@link #KEY_SET_B}, {@link #KEY_SET_C}, and/or
   * {@link #KEY_SET_F}.
   * <p>
   * Some tests are dependent on the order of the keys/entries.  In general,
   * for each key set ('xxxXn'), the keys/entries must be ordered by 'n'.
   */
  static final Map<String, String> TEST_ENTRIES;
  static {
    final Map<String, String> testEntries = new LinkedHashMap<>();
    testEntries.put("keyA1", "valueA1");   // keySet A
    testEntries.put("keyB1", "valueB1");   // keySet B
    testEntries.put("keyC1", "valueC1");   // keySet C
    testEntries.put("keyD1", "valueD1");   // KeySet D
    testEntries.put("keyE1", "valueE1");   // KeySet E
    testEntries.put("keyF1", "valueF1");   // KeySet F

    testEntries.put("keyC2", "valueC2");   // keySet C
    testEntries.put("keyD2", "valueD2");   // KeySet D
    testEntries.put("keyA2", "valueA2");   // keySet A
    testEntries.put("keyB2", "valueB2");   // ketSet B
    testEntries.put("keyF2", "valueF2");   // keySet F
    testEntries.put("keyE2", "valueE2");   // KeySet E

    testEntries.put("keyD3", "valueD3");   // KeySet D
    testEntries.put("keyC3", "valueC3");   // keySet C
    testEntries.put("keyF3", "valueF3");   // keySet F
    testEntries.put("keyE3", "valueE3");   // KeySet E
    testEntries.put("keyA3", "valueA3");   // keySet A
    testEntries.put("keyB3", "valueB3");   // ketSet B

    testEntries.put("keyF4", "valueF4");   // KeySet F
    testEntries.put("keyE4", "valueE4");   // KeySet E
    testEntries.put("keyD4", "valueD4");   // KeySet D
    testEntries.put("keyC4", "valueC4");   // keySet C
    testEntries.put("keyB4", "valueB4");   // keySet B
    testEntries.put("keyA4", "valueA4");   // keySet A

    testEntries.put("keyE5", "valueE5");   // KeySet E
    testEntries.put("keyF5", "valueF5");   // KeySet F
    testEntries.put("keyB5", "valueB5");   // keySet B
    testEntries.put("keyA5", "valueA5");   // keySet A
    testEntries.put("keyD5", "valueD5");   // KeySet D
    testEntries.put("keyC5", "valueC5");   // keySet C

    testEntries.put("keyB6", "valueB6");   // keySet B
    testEntries.put("keyA6", "valueA6");   // keySet A
    testEntries.put("keyE6", "valueE6");   // KeySet E
    testEntries.put("keyF6", "valueF6");   // KeySet F
    testEntries.put("keyC6", "valueC6");   // keySet C
    testEntries.put("keyD6", "valueD6");   // KeySet D

    testEntries.put("keyB7", "valueB7");   // keySet B
    testEntries.put("keyE7", "valueE7");   // KeySet E
    testEntries.put("keyF7", "valueF7");   // KeySet F
    testEntries.put("keyC7", "valueC7");   // keySet C
    testEntries.put("keyD7", "valueD7");   // KeySet D

    testEntries.put("keyE8", "valueE8");   // KeySet E
    testEntries.put("keyF8", "valueF8");   // KeySet F
    testEntries.put("keyC8", "valueC8");   // keySet C
    testEntries.put("keyD8", "valueD8");   // KeySet D

    testEntries.put("keyF9", "valueF9");   // KeySet F
    testEntries.put("keyC9", "valueC9");   // keySet C
    testEntries.put("keyD9", "valueD9");   // KeySet D

    testEntries.put("keyC10", "valueC10");   // keySet C
    testEntries.put("keyD10", "valueD10");   // KeySet D

    testEntries.put("keyD11", "valueD11");   // KeySet D

    TEST_ENTRIES = Collections.unmodifiableMap(testEntries);
  }

  /** The {@link #TEST_ENTRIES} key subset matching {@code xxxAx}. */
  static final Set<String> KEY_SET_A;
  /** The {@link #TEST_ENTRIES} key subset matching {@code xxxBx}. */
  static final Set<String> KEY_SET_B;
  /** The {@link #TEST_ENTRIES} key subset matching {@code xxxCx}. */
  static final Set<String> KEY_SET_C;
  /** The {@link #TEST_ENTRIES} key subset matching {@code xxxDx}. */
  static final Set<String> KEY_SET_D;
  /** The {@link #TEST_ENTRIES} key subset matching {@code xxxEx}. */
  static final Set<String> KEY_SET_E;
  /** The {@link #TEST_ENTRIES} key subset matching {@code xxxFx}. */
  static final Set<String> KEY_SET_F;
  static {
    final Set<String> keySetA = new LinkedHashSet<>();
    final Set<String> keySetB = new LinkedHashSet<>();
    final Set<String> keySetC = new LinkedHashSet<>();
    final Set<String> keySetD = new LinkedHashSet<>();
    final Set<String> keySetE = new LinkedHashSet<>();
    final Set<String> keySetF = new LinkedHashSet<>();

    for (final String key : TEST_ENTRIES.keySet()) {
      final char set = key.charAt(3);
      switch (set) {
        case 'A':
          keySetA.add(key);
          break;
        case 'B':
          keySetB.add(key);
          break;
        case 'C':
          keySetC.add(key);
          break;
        case 'D':
          keySetD.add(key);
          break;
        case 'E':
          keySetE.add(key);
          break;
        case 'F':
          keySetF.add(key);
          break;
        default:
          throw new AssertionError();
      }
    }

    KEY_SET_A = Collections.unmodifiableSet(keySetA);
    KEY_SET_B = Collections.unmodifiableSet(keySetB);
    KEY_SET_C = Collections.unmodifiableSet(keySetC);
    KEY_SET_D = Collections.unmodifiableSet(keySetD);
    KEY_SET_E = Collections.unmodifiableSet(keySetE);
    KEY_SET_F = Collections.unmodifiableSet(keySetF);
  }

  /**
   * Gets a {@code Map} holding entries corresponding to the key {@code Iterable} provided.
   *
   * @param subset the {@code Iterable} over the keys for entries to copy into the result {@code Map}
   *
   * @return a new, insert-ordered, modifiable {@code Map} holding the designated entries
   */
  static Map<String, String> getEntryMap(final Iterable<String> subset) {
    return getEntryMap(Collections.singletonList(subset), null);
  }

  /**
   * Gets a {@code Map} holding entries corresponding to the key {@code Iterable}s provided.
   *
   * @param subset1 the first {@code Iterable} over the keys for entries to copy into the result {@code Map}
   * @param subset2 the second {@code Iterable}s over the keys for entries to copy into the result {@code Map}
   *
   * @return a new, insert-ordered, modifiable {@code Map} holding the designated entries
   */
  static Map<String, String> getEntryMap(final Iterable<String> subset1, final Iterable<String> subset2) {
    final List<Iterable<String>> subsets = new ArrayList<>(2);
    subsets.add(subset1);
    subsets.add(subset2);
    return getEntryMap(subsets, null);
  }

  /**
   * Gets a {@code Map} holding entries corresponding to the key {@code Iterable}s provided.
   *
   * @param subset1 the first {@code Iterable} over the keys for entries to copy into the result {@code Map}
   * @param subset2 the second {@code Iterable}s over the keys for entries to copy into the result {@code Map}
   * @param subset3 the third {@code Iterable}s over the keys for entries to copy into the result {@code Map}
   *
   * @return a new, insert-ordered, modifiable {@code Map} holding the designated entries
   */
  static Map<String, String> getEntryMap(
      final Iterable<String> subset1, final Iterable<String> subset2, final Iterable<String> subset3) {
    final List<Iterable<String>> subsets = new ArrayList<>(3);
    subsets.add(subset1);
    subsets.add(subset2);
    subsets.add(subset3);
    return getEntryMap(subsets, null);
  }

  /**
   * Gets a {@code Map} holding entries corresponding to the key {@code Iterable}s provided.
   *
   * @param subset1 the first {@code Iterable} over the keys for entries to copy into the result {@code Map}
   * @param subset2 the second {@code Iterable}s over the keys for entries to copy into the result {@code Map}
   * @param subset3 the third {@code Iterable}s over the keys for entries to copy into the result {@code Map}
   * @param subset4 the fourth {@code Iterable}s over the keys for entries to copy into the result {@code Map}
   *
   * @return a new, insert-ordered, modifiable {@code Map} holding the designated entries
   */
  static Map<String, String> getEntryMap(
      final Iterable<String> subset1, final Iterable<String> subset2, final Iterable<String> subset3, final Iterable<String> subset4) {
    final List<Iterable<String>> subsets = new ArrayList<>(4);
    subsets.add(subset1);
    subsets.add(subset2);
    subsets.add(subset3);
    subsets.add(subset4);
    return getEntryMap(subsets, null);
  }

  /**
   * Gets a {@code Map} holding entries corresponding to the key {@code Iterable} provided.  Each entry
   * value is prepended with the prefix value provided.
   *
   * @param prefix the non-{@code null} prefix used to alter each entry's value
   * @param subset the {@code Iterable} over the keys for entries to copy into the result {@code Map}
   *
   * @return a new, insert-ordered, modifiable {@code Map} holding the designated entries
   */
  static Map<String, String> getAltEntryMap(final String prefix, final Iterable<String> subset) {
    assert prefix != null;
    return getEntryMap(Collections.singletonList(subset), prefix);
  }

  /**
   * Gets a {@code Map} holding entries corresponding to the key {@code Iterable}s provided.
   *
   * @param subsets the {@code Iterable}s over the keys for entries to copy into the result {@code Map}
   * @param prefix the value to prepend to each entry value; if {@code null}, the values are not altered
   *
   * @return a new, insert-ordered, modifiable {@code Map} holding the designated entries
   */
  private static Map<String, String> getEntryMap(final List<Iterable<String>> subsets, final String prefix) {
    final StringBuilder sb = (prefix == null ? null : new StringBuilder(prefix));
    final Map<String, String> entryMap = new LinkedHashMap<>();
    for (final Iterable<String> subset : subsets) {
      for (final String key : subset) {
        String value = TEST_ENTRIES.get(key);
        if (prefix != null) {
          sb.setLength(prefix.length());
          value = sb.append(value).toString();
        }
        entryMap.put(key, value);
      }
    }
    return entryMap;
  }

  /**
   * Gets a {@code Map} having a {@code null} value for each of the keys provided in {@code keySet}.
   *
   * @param keySet the {@code Iterable} over the keys for the result {@code Map}
   *
   * @return a new, insert-ordered, modifiable {@code Map} holding {@code null} values for each key provided
   */
  static Map<String, String> getNullEntryMap(final Iterable<String> keySet) {
    return getNullEntryMap(Collections.singletonList(keySet));
  }

  /**
   * Gets a {@code Map} having a {@code null} value for each of the keys provided all identified keySets.
   *
   * @param keySet1 the first {@code Iterable} over the keys for the result {@code Map}
   * @param keySet2 the second {@code Iterable} over the keys for the result {@code Map}
   *
   * @return a new, insert-ordered, modifiable {@code Map} holding {@code null} values for each key provided
   */
  static Map<String, String> getNullEntryMap(final Iterable<String> keySet1, final Iterable<String> keySet2) {
    final List<Iterable<String>> keySets = new ArrayList<>(2);
    keySets.add(keySet1);
    keySets.add(keySet2);
    return getNullEntryMap(keySets);
  }

  /**
   * Gets a {@code Map} having a {@code null} value for each of the keys provided in {@code keySets}.
   *
   * @param keySets the {@code Iterable}s over the keys for the result {@code Map}
   *
   * @return a new, insert-ordered, modifiable {@code Map} holding {@code null} values for each key provided
   */
  private static Map<String, String> getNullEntryMap(final List<Iterable<String>> keySets) {
    final Map<String, String> entryMap = new LinkedHashMap<>();
    for (final Iterable<String> keySet : keySets) {
      for (final String key : keySet) {
        entryMap.put(key, null);
      }
    }
    return entryMap;
  }

  /**
   * Gets a {@code Map} containing all of the entries from the maps provided.
   *
   * @param map1 the first {@code Map} instance to combine
   * @param map2 the second {@code Map} instance to combine
   *
   * @return a new {@code Map} containing all of the entries
   */
  static Map<String, String> union(final Map<String, String> map1, final Map<String, String> map2) {
    final List<Map<String, String>> maps = new ArrayList<>();
    maps.add(map1);
    maps.add(map2);
    return unionMaps(maps);
  }

  /**
   * Gets a {@code Map} containing all of the entries from the maps provided.
   *
   * @param map1 the first {@code Map} instance to combine
   * @param map2 the second {@code Map} instance to combine
   * @param map3 the third {@code Map} instance to combine
   *
   * @return a new {@code Map} containing all of the entries
   */
  static Map<String, String> union(
      final Map<String, String> map1, final Map<String, String> map2, final Map<String, String> map3) {
    final List<Map<String, String>> maps = new ArrayList<>();
    maps.add(map1);
    maps.add(map2);
    maps.add(map3);
    return unionMaps(maps);
  }

  /**
   * Gets a {@code Map} containing all of the entries from the maps provided.
   *
   * @param maps the {@code Map} instances to combine
   *
   * @return a new {@code Map} containing all of the entries
   */
  private static Map<String, String> unionMaps(final List<Map<String, String>> maps) {
    final Map<String, String> union = new HashMap<>();
    for (Map<String, String> map : maps) {
      union.putAll(map);
    }
    return union;
  }

  /**
   * Gets a {@code Set} containing all of the elements from the sets provided
   *
   * @param set1 the first {@code Set} instance to combine
   * @param set2 the second {@code Set} instance to combine
   *
   * @return a new {@code Set} containing all of the elements
   */
  static Set<String> union(final Set<String> set1, final Set<String> set2) {
    final List<Set<String>> sets = new ArrayList<>();
    sets.add(set1);
    sets.add(set2);
    return unionSets(sets);
  }

  /**
   * Gets a {@code Set} containing all of the entries from the sets provided.
   *
   * @param sets the {@code Set} instances to combine
   *
   * @return a new {@code Set} containing all of the elements
   */
  private static Set<String> unionSets(final List<Set<String>> sets) {
    final Set<String> union = new HashSet<>();
    for (final Set<String> set : sets) {
      union.addAll(set);
    }
    return union;
  }

  /**
   * Gets a new {@code Set} that is the interleaved union of the {@code Set}s provided.
   *
   * @param keySet1 the first {@code Set} of keys to appear in the result {@code Set}
   * @param keySet2 the second {@code Set} of keys to appear in the result {@code Set}
   *
   * @return a new, modifiable {@code Set} holding the designated keys
   *
   * @see #fanIn(java.util.List)
   */
  static Set<String> fanIn(final Set<String> keySet1, final Set<String> keySet2) {
    final List<Set<String>> keySets = new ArrayList<>();
    keySets.add(keySet1);
    keySets.add(keySet2);
    return fanIn(keySets);
  }

  /**
   * Gets a new {@code Set} that is the interleaved union of the {@code Set}s provided.
   *
   * @param keySet1 the first {@code Set} of keys to appear in the result {@code Set}
   * @param keySet2 the second {@code Set} of keys to appear in the result {@code Set}
   * @param keySet3 the third {@code Set} of keys to appear in the result {@code Set}
   *
   * @return a new, modifiable {@code Set} holding the designated keys
   *
   * @see #fanIn(java.util.List)
   */
  static Set<String> fanIn(final Set<String> keySet1, final Set<String> keySet2, final Set<String> keySet3) {
    final List<Set<String>> keySets = new ArrayList<>();
    keySets.add(keySet1);
    keySets.add(keySet2);
    keySets.add(keySet3);
    return fanIn(keySets);
  }

  /**
   * Gets a new {@code Set} that is the interleaved union of the {@code Set}s provided.
   *
   * @param keySet1 the first {@code Set} of keys to appear in the result {@code Set}
   * @param keySet2 the second {@code Set} of keys to appear in the result {@code Set}
   * @param keySet3 the third {@code Set} of keys to appear in the result {@code Set}
   * @param keySet4 the fourth {@code Set} of keys to appear in the result {@code Set}
   *
   * @return a new, modifiable {@code Set} holding the designated keys
   *
   * @see #fanIn(java.util.List)
   */
  static Set<String> fanIn(
      final Set<String> keySet1, final Set<String> keySet2, final Set<String> keySet3, final Set<String> keySet4) {
    final List<Set<String>> keySets = new ArrayList<>();
    keySets.add(keySet1);
    keySets.add(keySet2);
    keySets.add(keySet3);
    keySets.add(keySet4);
    return fanIn(keySets);
  }

  /**
   * Gets a new {@code Set} that is the interleaved union of the {@code Set}s provided.  The
   * set returned from this operation is in the following order:
   * <ul>
   *   <li>keySet[0][0]</li>
   *   <li>keySet[1][0]</li>
   *   <li>...</li>
   *   <li>keySet[N][0]</li>
   *   <li>keySet[0][1]</li>
   *   <li>keySet[1][1]</li>
   *   <li>...</li>
   *   <li>keySet[N][1]</li>
   *   <li>keySet[0][2]</li>
   *   <li>keySet[1][2]</li>
   *   <li>...</li>
   *   <li>keySet[N][2]</li>
   *   <li>...</li>
   * </ul>
   * where {@code keySet[0][0]} is the first item returned from the {@code Iterator} obtained from
   * {@code {@code keySets.get(0).iterator()}, {@code keySet[1][0]} is the first item obtained from
   * {@code {@code keySets.get(1).iterator()}, etc.

   *
   * @param keySets the {@code Set}s of keys to appear in the result {@code Set}
   *
   * @return a new, modifiable {@code Set} holding the designated keys
   */
  private static Set<String> fanIn(final List<Set<String>> keySets) {
    final Set<String> union = new LinkedHashSet<>();

    /*
     * Collect the keys from the sets provided in the iteration order of the main
     * test entry map.
     */
    for (final String key : TEST_ENTRIES.keySet()) {
      for (final Set<String> keySet : keySets) {
        if (keySet.contains(key)) {
          union.add(key);
          break;
        }
      }
    }
    return union;
  }

  /**
   * Copies entries from the {@code Set} provided until reaching a barrier item.
   * The items are copied in iterator order and the result does not include the barrier.
   *
   * @param source the {@code Set} from which the items are copied
   * @param barrier the barrier indicating the end of the copy
   *
   * @return a new, entry-ordered {@code Set} containing the subset of {@code source}
   */
  static Set<String> copyUntil(final Set<String> source, final String barrier) {
    final Set<String> subset = new LinkedHashSet<>();
    for (final String sourceItem : source) {
      if (barrier.equals(sourceItem)) {
        break;
      }
      subset.add(sourceItem);
    }
    return subset;
  }

  /**
   * Makes a shallow copy of the {@code Set} provided retaining only those elements identified
   * by in the {@code Collection} {@code retained}.
   *
   * @param source the {@code Set} from which the copy is made
   * @param retained the elements to keep in the copy
   *
   * @return a new, entry-ordered {@code Set} containing the just the retained elements
   */
  static Set<String> copyOnly(final Set<String> source, final Collection<String> retained) {
    final Set<String> copy = new LinkedHashSet<>(source);
    copy.retainAll(retained);
    return copy;
  }

  /**
   * Makes a shallow copy of the {@code Set} provided omitting those elements identified by the
   * {@code Collection} {@code removed}.
   *
   * @param source the {@code Set} from which the copy is made
   * @param removed the elements to omit from the copy
   *
   * @return a new, entry-ordered {@code Set} containing the elements from {@code source} less those
   *    specified in {@code removed}
   */
  static Set<String> copyWithout(final Set<String> source, final Collection<String> removed) {
    final Set<String> copy = new LinkedHashSet<>(source);
    copy.removeAll(removed);
    return copy;
  }

  /**
   * Makes a shallow copy of the {@code Map} provided retaining only those entries identified
   * by the keys in the {@code Collection} {@code retained}.
   *
   * @param source the {@code Map} from which the copy is made
   * @param retained the keys of the entries to keep in the copy
   *
   * @return a new, entry-ordered {@code Map} containing the just the retained entries
   */
  static Map<String, String> copyOnly(final Map<String, String> source, final Collection<String> retained) {
    final Map<String, String> copy = new LinkedHashMap<>(source);
    copy.keySet().retainAll(retained);
    return copy;
  }

  /**
   * Makes a shallow copy of the {@code Map} provided omitting those entries identified by the
   * keys in the {@code Collection} {@code removed}.
   *
   * @param source the {@code Map} from which the copy is made
   * @param removed the keys of the entries to omit from the copy
   *
   * @return a new, entry-ordered {@code Map} containing the entries from {@code source} less those
   *    specified in {@code removed}
   */
  static Map<String, String> copyWithout(final Map<String, String> source, final Collection<String> removed) {
    final Map<String, String> copy = new LinkedHashMap<>(source);
    copy.keySet().removeAll(removed);
    return copy;
  }
}
