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

package org.ehcache.integration;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.impl.config.copy.DefaultCopierConfiguration;
import org.ehcache.impl.config.serializer.DefaultSerializerConfiguration;
import org.ehcache.spi.serialization.SerializerException;
import org.ehcache.impl.copy.ReadWriteCopier;
import org.ehcache.impl.copy.SerializingCopier;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.serialization.Serializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;

/**
 * Created by alsu on 01/09/15.
 */
public class CacheCopierTest {

  CacheManager cacheManager;
  CacheConfigurationBuilder<Long, Person> baseConfig = newCacheConfigurationBuilder(Long.class, Person.class, heap(5));


  @Before
  public void setUp() throws Exception {
    cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .build(true);
  }

  @After
  public void tearDown() throws Exception {
    cacheManager.close();
  }

  @Test
  public void testCopyValueOnRead() throws Exception {
    CacheConfiguration<Long, Person> cacheConfiguration = baseConfig
        .withService(new DefaultCopierConfiguration<>(PersonOnReadCopier.class, DefaultCopierConfiguration.Type.VALUE))
        .build();

    Cache<Long, Person> cache = cacheManager.createCache("cache", cacheConfiguration);

    Person original = new Person("Bar", 24);
    cache.put(1l, original);

    Person retrieved = cache.get(1l);
    assertNotSame(original, retrieved);
    assertThat(retrieved.name, equalTo("Bar"));
    assertThat(retrieved.age, equalTo(24));

    original.age = 56;
    retrieved = cache.get(1l);
    assertThat(retrieved.age, equalTo(56));

    assertNotSame(cache.get(1l), cache.get(1l));
  }

  @Test
  public void testCopyValueOnWrite() throws Exception {
    CacheConfiguration<Long, Person> cacheConfiguration = baseConfig
        .withService(new DefaultCopierConfiguration<>(PersonOnWriteCopier.class, DefaultCopierConfiguration.Type.VALUE))
        .build();

    Cache<Long, Person> cache = cacheManager.createCache("cache", cacheConfiguration);

    Person original = new Person("Bar", 24);
    cache.put(1l, original);

    Person retrieved = cache.get(1l);
    assertNotSame(original, retrieved);
    assertThat(retrieved.name, equalTo("Bar"));
    assertThat(retrieved.age, equalTo(24));

    original.age = 56;
    retrieved = cache.get(1l);
    assertThat(retrieved.age, equalTo(24));

    assertSame(cache.get(1l), cache.get(1l));
  }

  @Test
  public void testIdentityCopier() throws Exception {
    CacheConfiguration<Long, Person> cacheConfiguration = baseConfig.build();

    Cache<Long, Person> cache = cacheManager.createCache("cache", cacheConfiguration);

    Person original = new Person("Bar", 24);
    cache.put(1l, original);

    Person retrieved = cache.get(1l);
    assertSame(original, retrieved);

    original.age = 25;
    retrieved = cache.get(1l);
    assertSame(original, retrieved);

    assertSame(cache.get(1l), cache.get(1l));
  }

  @Test
  public void testSerializingCopier() throws Exception {
    CacheConfiguration<Long, Person> cacheConfiguration = baseConfig
        .withService(new DefaultCopierConfiguration<>(SerializingCopier.<Person>asCopierClass(), DefaultCopierConfiguration.Type.VALUE))
        .withService(new DefaultSerializerConfiguration<>(PersonSerializer.class, DefaultSerializerConfiguration.Type.VALUE))
        .build();

    Cache<Long, Person> cache = cacheManager.createCache("cache", cacheConfiguration);

    Person original = new Person("Bar", 24);
    cache.put(1l, original);

    Person retrieved = cache.get(1l);
    assertNotSame(original, retrieved);
    assertThat(retrieved.name, equalTo("Bar"));
    assertThat(retrieved.age, equalTo(24));

    original.age = 56;
    retrieved = cache.get(1l);
    assertThat(retrieved.age, equalTo(24));

    assertNotSame(cache.get(1l), cache.get(1l));
  }

  @Test
  public void testReadWriteCopier() throws Exception {
    CacheConfiguration<Long, Person> cacheConfiguration = baseConfig
        .withService(new DefaultCopierConfiguration<>(PersonCopier.class, DefaultCopierConfiguration.Type.VALUE))
        .build();

    Cache<Long, Person> cache = cacheManager.createCache("cache", cacheConfiguration);

    Person original = new Person("Bar", 24);
    cache.put(1l, original);

    Person retrieved = cache.get(1l);
    assertNotSame(original, retrieved);
    assertThat(retrieved.name, equalTo("Bar"));
    assertThat(retrieved.age, equalTo(24));

    original.age = 56;
    retrieved = cache.get(1l);
    assertThat(retrieved.age, equalTo(24));

    assertNotSame(cache.get(1l), cache.get(1l));
  }

  private static class Description {
    int id;
    String alias;

    Description(Description other) {
      this.id = other.id;
      this.alias = other.alias;
    }

    Description(int id, String alias) {
      this.id = id;
      this.alias = alias;
    }

    @Override
    public boolean equals(final Object other) {
      if(this == other) return true;
      if(other == null || this.getClass() != other.getClass()) return false;

      Description that = (Description)other;
      if(id != that.id) return false;
      if ((alias == null) ? (alias != null) : !alias.equals(that.alias)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      int result = 1;
      result = 31 * result + id;
      result = 31 * result + (alias == null ? 0 : alias.hashCode());
      return result;
    }
  }

  private static class Person implements Serializable {

    private static final long serialVersionUID = 1L;

    String name;
    int age;

    Person(Person other) {
      this.name = other.name;
      this.age = other.age;
    }

    Person(String name, int age) {
      this.name = name;
      this.age = age;
    }

    @Override
    public boolean equals(final Object other) {
      if(this == other) return true;
      if(other == null || this.getClass() != other.getClass()) return false;

      Person that = (Person)other;
      if(age != that.age) return false;
      if((name == null) ? (that.name != null) : !name.equals(that.name)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = 1;
      result = 31 * result + age;
      result = 31 * result + (name == null ? 0 : name.hashCode());
      return result;
    }

    @Override
    public String toString() {
      return "Person[name: " + (name != null ? name : "") + ", age: " + age + "]";
    }
  }

  public static class DescriptionCopier extends ReadWriteCopier<Description> {

    @Override
    public Description copy(final Description obj) {
      return new Description(obj);
    }
  }

  public static class PersonOnReadCopier implements Copier<Person> {

    @Override
    public Person copyForRead(final Person obj) {
      return new Person(obj);
    }

    @Override
    public Person copyForWrite(final Person obj) {
      return obj;
    }
  }

  public static class PersonOnWriteCopier implements Copier<Person> {

    @Override
    public Person copyForRead(final Person obj) {
      return obj;
    }

    @Override
    public Person copyForWrite(final Person obj) {
      return new Person(obj);
    }
  }

  public static class PersonCopier extends ReadWriteCopier<Person> {

    @Override
    public Person copy(final Person obj) {
      return new Person(obj);
    }
  }

  public static class PersonSerializer implements Serializer<Person> {

    private static final Charset CHARSET = Charset.forName("US-ASCII");

    public PersonSerializer(ClassLoader loader) {
    }

    @Override
    public ByteBuffer serialize(final Person object) throws SerializerException {
      ByteBuffer buffer = ByteBuffer.allocate(object.name.length() + 4);
      buffer.putInt(object.age);
      buffer.put(object.name.getBytes(CHARSET)).flip();
      return buffer;
    }

    @Override
    public Person read(final ByteBuffer binary) throws ClassNotFoundException, SerializerException {
      int age = binary.getInt();
      byte[] bytes = new byte[binary.remaining()];
      binary.get(bytes);
      String name = new String(bytes, CHARSET);
      return new Person(name, age);
    }

    @Override
    public boolean equals(final Person object, final ByteBuffer binary) throws ClassNotFoundException, SerializerException {
      return object.equals(read(binary));
    }
  }

}
