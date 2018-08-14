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

package org.ehcache.core.spi.store;

import org.ehcache.core.spi.time.TimeSource;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * @author Ludovic Orban
 */
public class AbstractValueHolderTest {

  @Test
  public void testCreationTime() throws Exception {
    AbstractValueHolder<String> valueHolder = newAbstractValueHolder(1000L);

    assertThat(valueHolder.creationTime(), is(1000L));
  }

  @Test
  public void testExpirationTime() throws Exception {
    AbstractValueHolder<String> valueHolder = newAbstractValueHolder(0L, 1000L);

    assertThat(valueHolder.expirationTime(), is(1000L));
  }


  @Test
  public void testLastAccessTime() throws Exception {
    // last access time defaults to create time
    AbstractValueHolder<String> valueHolder = newAbstractValueHolder(1000L);

    assertThat(valueHolder.lastAccessTime(), is(1000L));

    valueHolder = newAbstractValueHolder(1000L, 0L, 2000L);

    assertThat(valueHolder.lastAccessTime(), is(2000L));
  }


  @Test
  public void testIsExpired() throws Exception {
    assertThat(newAbstractValueHolder(1000L).isExpired(1000L), is(false));

    assertThat(newAbstractValueHolder(1000L, 1001L).isExpired(1000L), is(false));

    assertThat(newAbstractValueHolder(1000L, 1000L).isExpired(1000L), is(true));
  }

  @Test
  public void testEquals() throws Exception {
    assertThat(newAbstractValueHolder( 0L).equals(newAbstractValueHolder( 0L)), is(true));
    assertThat(newAbstractValueHolder( 1L).equals(newAbstractValueHolder( 0L)), is(false));

    assertThat(newAbstractValueHolder(2L, 0L).equals(newAbstractValueHolder(2L, 0L)), is(true));
    assertThat(newAbstractValueHolder(2L, 0L).equals(newAbstractValueHolder(2L, 1L)), is(false));
    assertThat(newAbstractValueHolder(2L, 0L).equals(newAbstractValueHolder(3L, 0L)), is(false));

    assertThat(newAbstractValueHolder(0L, 2L, 1L).equals(newAbstractValueHolder(0L, 2L, 1L)), is(true));
    assertThat(newAbstractValueHolder(1L, 2L, 1L).equals(newAbstractValueHolder(0L, 2L, 1L)), is(false));

    assertThat(newAbstractValueHolder(0L, 3L, 1L).equals(newAbstractValueHolder(0L, 2L, 1L)), is(false));
    assertThat(newAbstractValueHolder(0L, 2L, 3L).equals(newAbstractValueHolder(0L, 2L, 1L)), is(false));
  }

  @Test
  public void testSubclassEquals() throws Exception {
    assertThat(new AbstractValueHolder<String>(-1, 1L) {
      @Override
      public String get() {
        return "aaa";
      }

      @Override
      public int hashCode() {
        return super.hashCode() + get().hashCode();
      }
      @Override
      public boolean equals(Object obj) {
        if (obj instanceof AbstractValueHolder) {
          AbstractValueHolder<?> other = (AbstractValueHolder<?>) obj;
          return super.equals(obj) && get().equals(other.get());
        }
        return false;
      }
    }.equals(new AbstractValueHolder<String>(-1, 1L) {
      @Override
      public String get() {
        return "aaa";
      }

      @Override
      public int hashCode() {
        return super.hashCode() + get().hashCode();
      }

      @Override
      public boolean equals(Object obj) {
        if (obj instanceof AbstractValueHolder) {
          AbstractValueHolder<?> other = (AbstractValueHolder<?>)obj;
          return super.equals(obj) && get().equals(other.get());
        }
        return false;
      }
    }), is(true));

  }

  private AbstractValueHolder<String> newAbstractValueHolder(long creationTime) {
    return new AbstractValueHolder<String>(-1, creationTime) {
      @Override
      public String get() {
        throw new UnsupportedOperationException();
      }
    };
  }

  private AbstractValueHolder<String> newAbstractValueHolder(long creationTime, long expirationTime) {
    return new AbstractValueHolder<String>(-1, creationTime, expirationTime) {
      @Override
      public String get() {
        throw new UnsupportedOperationException();
      }
    };
  }

  private AbstractValueHolder<String> newAbstractValueHolder(long creationTime, long expirationTime, long lastAccessTime) {
    final AbstractValueHolder<String> abstractValueHolder = new AbstractValueHolder<String>(-1, creationTime, expirationTime) {
      @Override
      public String get() {
        throw new UnsupportedOperationException();
      }
    };
    abstractValueHolder.setLastAccessTime(lastAccessTime);
    return abstractValueHolder;
  }

  private static class TestTimeSource implements TimeSource {

    private long time = 0;

    @Override
    public long getTimeMillis() {
      return time;
    }

    public void advanceTime(long step) {
      time += step;
    }
  }

}
