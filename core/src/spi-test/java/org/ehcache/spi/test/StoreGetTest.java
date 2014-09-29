package org.ehcache.spi.test;

import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.spi.cache.Store;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.ehcache.spi.cache.Store.ValueHolder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.fail;


/**
 * Test the {@link org.ehcache.spi.cache.Store#get(K key)} contract of the
 * {@link org.ehcache.spi.cache.Store Store} interface.
 * <p/>
 *
 * @author Aurelien Broszniowski
 */

public class StoreGetTest<K, V> extends SPIStoreTester<K, V> {

  private final Class<K> keyClass;
  private final Class<V> valueClass;

  public StoreGetTest(final StoreFactory factory, Class<K> keyClass, Class<V> valueClass) {
    super(factory);
    this.keyClass = keyClass;
    this.valueClass = valueClass;
  }

  @SPITest
  public void existingKeyMappedInStoreReturnsValueHolder()
      throws CacheAccessException, IllegalAccessException, InstantiationException {
    final Store<K, V> kvStore  = storeFactory.newStore(new StoreConfigurationImpl<K, V>(keyClass, valueClass));

    K key = keyClass.newInstance();
    V value = valueClass.newInstance();

    kvStore.put(key, value);

    assertThat(kvStore.get(key), is(instanceOf(ValueHolder.class)));
  }

  @SPITest
  public void keyNotMappedInStoreReturnsNull()
      throws CacheAccessException, IllegalAccessException, InstantiationException {
    final Store<K, V> kvStore  = storeFactory.newStore(new StoreConfigurationImpl<K, V>(keyClass, valueClass));

    K key = keyClass.newInstance();

    assertThat(kvStore.get(key), is(nullValue()));
  }

  @SPITest
  public void existingKeyMappedInStoreReturnsCorrectValueHolder()
      throws CacheAccessException, IllegalAccessException, InstantiationException {
    final Store<K, V> kvStore  = storeFactory.newStore(new StoreConfigurationImpl<K, V>(keyClass, valueClass));

    K key = keyClass.newInstance();
    V value = valueClass.newInstance();

    kvStore.put(key, value);
    ValueHolder<V> returnedValueHolder = kvStore.get(key);

    assertThat(returnedValueHolder.value(), is(equalTo(value)));
  }

  @SPITest
  public void nullKeyThrowsException()
      throws CacheAccessException, IllegalAccessException, InstantiationException {
    final Store<K, V> kvStore  = storeFactory.newStore(new StoreConfigurationImpl<K, V>(keyClass, valueClass));

    K key = null;

    try {
      kvStore.get(key);
      fail("Expected NullPointerException because the key is null");
    } catch (NullPointerException e) {
      // expected
    }
  }

  @SPITest
  @SuppressWarnings("unchecked")
  public void wrongKeyTypeThrowsException()
      throws CacheAccessException, IllegalAccessException, InstantiationException {
    final Store kvStore  = storeFactory.newStore(new StoreConfigurationImpl<K, V>(keyClass, valueClass));

    try {
      if (keyClass == String.class) {
        kvStore.get(1.0f);
      } else {
        kvStore.get("key");
      }
      fail("Expected ClassCastException because the key is of the wrong type");
    } catch (ClassCastException e) {
      System.out.println("------/." + e.getMessage());
      assertThat(e.getMessage(), is(equalTo("Argument foo is not valid!")));
    }
  }

  @SPITest
  public void retrievalCanThrowException()
      throws IllegalAccessException, InstantiationException {
    final Store<K, V> kvStore  = storeFactory.newStore(new StoreConfigurationImpl<K, V>(keyClass, valueClass));

    K key = keyClass.newInstance();

    try {
      kvStore.get(key);
    } catch (CacheAccessException e) {
      // This will not compile if the CacheAccessException is not thrown by the get(K key) method
    }
  }

}