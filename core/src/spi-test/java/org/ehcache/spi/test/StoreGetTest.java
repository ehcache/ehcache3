package org.ehcache.spi.test;

import org.ehcache.Cache;
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

  public StoreGetTest(final StoreFactory<K, V> factory) {
    super(factory);
  }

  @SPITest
  public void existingKeyMappedInStoreReturnsValueHolder()
      throws CacheAccessException, IllegalAccessException, InstantiationException {
    final Store<K, V> kvStore = factory.newStore(new StoreConfigurationImpl<K, V>(
        factory.getKeyType(), factory.getValueType(), null, new AlwaysTruePredicate<Cache.Entry<K, V>>(), null));

    K key = factory.getKeyType().newInstance();
    V value = factory.getValueType().newInstance();

    kvStore.put(key, value);

    assertThat(kvStore.get(key), is(instanceOf(ValueHolder.class)));
  }

  @SPITest
  public void keyNotMappedInStoreReturnsNull()
      throws CacheAccessException, IllegalAccessException, InstantiationException {
    final Store<K, V> kvStore = factory.newStore(
        new StoreConfigurationImpl<K, V>(this.factory.getKeyType(), this.factory.getValueType()));

    K key = factory.getKeyType().newInstance();

    assertThat(kvStore.get(key), is(nullValue()));
  }

  @SPITest
  public void existingKeyMappedInStoreReturnsCorrectValueHolder()
      throws CacheAccessException, IllegalAccessException, InstantiationException {
    final Store<K, V> kvStore = factory.newStore(new StoreConfigurationImpl<K, V>(
        factory.getKeyType(), factory.getValueType(), null, new AlwaysTruePredicate<Cache.Entry<K, V>>(), null));

    K key = factory.getKeyType().newInstance();
    V value = factory.getValueType().newInstance();

    kvStore.put(key, value);
    ValueHolder<V> returnedValueHolder = kvStore.get(key);

    assertThat(returnedValueHolder.value(), is(equalTo(value)));
  }

  @SPITest
  public void nullKeyThrowsException()
      throws CacheAccessException, IllegalAccessException, InstantiationException {
    final Store<K, V> kvStore = factory.newStore(
        new StoreConfigurationImpl<K, V>(factory.getKeyType(), factory.getValueType()));

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
    final Store kvStore = factory.newStore(
        new StoreConfigurationImpl<K, V>(factory.getKeyType(), factory.getValueType()));

    try {
      if (this.factory.getKeyType() == String.class) {
        kvStore.get(1.0f);
      } else {
        kvStore.get("key");
      }
      fail("Expected ClassCastException because the key is of the wrong type");
    } catch (ClassCastException e) {
      //TODO : consider adding some assertion on the exception getMessage() when the test passes
    }
  }

  @SPITest
  public void retrievalCanThrowException()
      throws IllegalAccessException, InstantiationException {
    final Store<K, V> kvStore = factory.newStore(
        new StoreConfigurationImpl<K, V>(factory.getKeyType(), factory.getValueType()));

    K key = factory.getKeyType().newInstance();

    try {
      kvStore.get(key);
    } catch (CacheAccessException e) {
      // This will not compile if the CacheAccessException is not thrown by the get(K key) method
    }
  }

}