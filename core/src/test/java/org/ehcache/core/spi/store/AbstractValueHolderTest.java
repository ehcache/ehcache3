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

import org.ehcache.expiry.Duration;
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
    AbstractValueHolder<String> valueHolder = newAbstractValueHolder(TimeUnit.MILLISECONDS, 1000L);

    assertThat(valueHolder.creationTime(TimeUnit.SECONDS), is(1L));
    assertThat(valueHolder.creationTime(TimeUnit.MILLISECONDS), is(1000L));
    assertThat(valueHolder.creationTime(TimeUnit.MICROSECONDS), is(1000000L));
  }

  @Test
  public void testExpirationTime() throws Exception {
    AbstractValueHolder<String> valueHolder = newAbstractValueHolder(TimeUnit.MILLISECONDS, 0L, 1000L);

    assertThat(valueHolder.expirationTime(TimeUnit.SECONDS), is(1L));
    assertThat(valueHolder.expirationTime(TimeUnit.MILLISECONDS), is(1000L));
    assertThat(valueHolder.expirationTime(TimeUnit.MICROSECONDS), is(1000000L));
  }


  @Test
  public void testLastAccessTime() throws Exception {
    // last access time defaults to create time
    AbstractValueHolder<String> valueHolder = newAbstractValueHolder(TimeUnit.MILLISECONDS, 1000L);

    assertThat(valueHolder.lastAccessTime(TimeUnit.SECONDS), is(1L));
    assertThat(valueHolder.lastAccessTime(TimeUnit.MILLISECONDS), is(1000L));
    assertThat(valueHolder.lastAccessTime(TimeUnit.MICROSECONDS), is(1000000L));

    valueHolder = newAbstractValueHolder(TimeUnit.MILLISECONDS, 1000L, 0L, 2000L);

    assertThat(valueHolder.lastAccessTime(TimeUnit.SECONDS), is(2L));
    assertThat(valueHolder.lastAccessTime(TimeUnit.MILLISECONDS), is(2000L));
    assertThat(valueHolder.lastAccessTime(TimeUnit.MICROSECONDS), is(2000000L));
  }


  @Test
  public void testIsExpired() throws Exception {
    assertThat(newAbstractValueHolder(TimeUnit.MILLISECONDS, 1000L).isExpired(1L, TimeUnit.SECONDS), is(false));
    assertThat(newAbstractValueHolder(TimeUnit.MILLISECONDS, 1000L).isExpired(1000L, TimeUnit.MILLISECONDS), is(false));
    assertThat(newAbstractValueHolder(TimeUnit.MILLISECONDS, 1000L).isExpired(1000000L, TimeUnit.MICROSECONDS), is(false));

    assertThat(newAbstractValueHolder(TimeUnit.MILLISECONDS, 1000L, 1001L).isExpired(1L, TimeUnit.SECONDS), is(false));
    assertThat(newAbstractValueHolder(TimeUnit.MILLISECONDS, 1000L, 1001L).isExpired(1000L, TimeUnit.MILLISECONDS), is(false));
    assertThat(newAbstractValueHolder(TimeUnit.MILLISECONDS, 1000L, 1001L).isExpired(1000000L, TimeUnit.MICROSECONDS), is(false));

    assertThat(newAbstractValueHolder(TimeUnit.MILLISECONDS, 1000L, 1000L).isExpired(1L, TimeUnit.SECONDS), is(true));
    assertThat(newAbstractValueHolder(TimeUnit.MILLISECONDS, 1000L, 1000L).isExpired(1000L, TimeUnit.MILLISECONDS), is(true));
    assertThat(newAbstractValueHolder(TimeUnit.MILLISECONDS, 1000L, 1000L).isExpired(1000000L, TimeUnit.MICROSECONDS), is(true));
  }

  @Test
  public void testEquals() throws Exception {
    assertThat(newAbstractValueHolder(TimeUnit.MILLISECONDS, 0L).equals(newAbstractValueHolder(TimeUnit.MILLISECONDS, 0L)), is(true));
    assertThat(newAbstractValueHolder(TimeUnit.MILLISECONDS, 1L).equals(newAbstractValueHolder(TimeUnit.MILLISECONDS, 0L)), is(false));

    assertThat(newAbstractValueHolder(TimeUnit.MILLISECONDS, 2L, 0L).equals(newAbstractValueHolder(TimeUnit.MILLISECONDS, 2L, 0L)), is(true));
    assertThat(newAbstractValueHolder(TimeUnit.MILLISECONDS, 2L, 0L).equals(newAbstractValueHolder(TimeUnit.MILLISECONDS, 2L, 1L)), is(false));
    assertThat(newAbstractValueHolder(TimeUnit.MILLISECONDS, 2L, 0L).equals(newAbstractValueHolder(TimeUnit.MILLISECONDS, 3L, 0L)), is(false));

    assertThat(newAbstractValueHolder(TimeUnit.MILLISECONDS, 0L, 2L, 1L).equals(newAbstractValueHolder(TimeUnit.MILLISECONDS, 0L, 2L, 1L)), is(true));
    assertThat(newAbstractValueHolder(TimeUnit.MILLISECONDS, 1L, 2L, 1L).equals(newAbstractValueHolder(TimeUnit.MILLISECONDS, 0L, 2L, 1L)), is(false));

    assertThat(newAbstractValueHolder(TimeUnit.MILLISECONDS, 0L, 3L, 1L).equals(newAbstractValueHolder(TimeUnit.MILLISECONDS, 0L, 2L, 1L)), is(false));
    assertThat(newAbstractValueHolder(TimeUnit.MILLISECONDS, 0L, 2L, 3L).equals(newAbstractValueHolder(TimeUnit.MILLISECONDS, 0L, 2L, 1L)), is(false));

    assertThat(newAbstractValueHolder(TimeUnit.MILLISECONDS, 1L).equals(newAbstractValueHolder(TimeUnit.SECONDS, 1L)), is(false));
    assertThat(newAbstractValueHolder(TimeUnit.NANOSECONDS, 1L).equals(newAbstractValueHolder(TimeUnit.SECONDS, 0L)), is(false));
    assertThat(newAbstractValueHolder(TimeUnit.SECONDS, 0L).equals(newAbstractValueHolder(TimeUnit.NANOSECONDS, 1L)), is(false));
    assertThat(newAbstractValueHolder(TimeUnit.SECONDS, 1L).equals(newAbstractValueHolder(TimeUnit.MILLISECONDS, 1000L)), is(true));
    assertThat(newAbstractValueHolder(TimeUnit.MILLISECONDS, 1000L).equals(newAbstractValueHolder(TimeUnit.SECONDS, 1L)), is(true));
  }

  @Test
  public void testSubclassEquals() throws Exception {
    assertThat(new AbstractValueHolder<String>(-1, 1000L) {
      @Override
      public String value() {
        return "aaa";
      }

      @Override
      protected TimeUnit nativeTimeUnit() {
        return TimeUnit.MILLISECONDS;
      }

      @Override
      public int hashCode() {
        return super.hashCode() + value().hashCode();
      }
      @Override
      public boolean equals(Object obj) {
        if (obj instanceof AbstractValueHolder) {
          AbstractValueHolder<?> other = (AbstractValueHolder<?>) obj;
          return super.equals(obj) && value().equals(other.value());
        }
        return false;
      }
    }.equals(new AbstractValueHolder<String>(-1, 1L) {
      @Override
      public String value() {
        return "aaa";
      }

      @Override
      protected TimeUnit nativeTimeUnit() {
        return TimeUnit.SECONDS;
      }

      @Override
      public int hashCode() {
        return super.hashCode() + value().hashCode();
      }

      @Override
      public boolean equals(Object obj) {
        if (obj instanceof AbstractValueHolder) {
          AbstractValueHolder<?> other = (AbstractValueHolder<?>)obj;
          return super.equals(obj) && value().equals(other.value());
        }
        return false;
      }
    }), is(true));

    assertThat(new AbstractValueHolder<String>(-1, 1000L) {
      @Override
      public String value() {
        return "aaa";
      }

      @Override
      protected TimeUnit nativeTimeUnit() {
        return TimeUnit.MICROSECONDS;
      }

      @Override
      public int hashCode() {
        return super.hashCode() + value().hashCode();
      }
      @Override
      public boolean equals(Object obj) {
        if (obj instanceof AbstractValueHolder) {
          AbstractValueHolder<?> other = (AbstractValueHolder<?>) obj;
          return super.equals(obj) && value().equals(other.value());
        }
        return false;
      }
    }.equals(new AbstractValueHolder<String>(-1, 1L) {
      @Override
      public String value() {
        return "bbb";
      }

      @Override
      protected TimeUnit nativeTimeUnit() {
        return TimeUnit.MILLISECONDS;
      }

      @Override
      public int hashCode() {
        return super.hashCode() + value().hashCode();
      }

      @Override
      public boolean equals(Object obj) {
        if (obj instanceof AbstractValueHolder) {
          AbstractValueHolder<?> other = (AbstractValueHolder<?>)obj;
          return super.equals(obj) && value().equals(other.value());
        }
        return false;
      }
    }), is(false));
  }

  @Test
  public void testAbstractValueHolderHitRate() {
    TestTimeSource timeSource = new TestTimeSource();
    timeSource.advanceTime(1);
    AbstractValueHolder<String> valueHolder = new AbstractValueHolder<String>(-1, timeSource.getTimeMillis()) {
      @Override
      protected TimeUnit nativeTimeUnit() {
        return TimeUnit.MILLISECONDS;
      }

      @Override
      public String value() {
        return "abc";
      }
    };
    valueHolder.accessed((timeSource.getTimeMillis()), new Duration(1L, TimeUnit.MILLISECONDS));
    timeSource.advanceTime(1000);
    assertThat(valueHolder.hitRate(timeSource.getTimeMillis(),
        TimeUnit.SECONDS), is(1.0f));
  }


  private AbstractValueHolder<String> newAbstractValueHolder(final TimeUnit timeUnit, long creationTime) {
    return new AbstractValueHolder<String>(-1, creationTime) {
      @Override
      protected TimeUnit nativeTimeUnit() {
        return timeUnit;
      }
      @Override
      public String value() {
        throw new UnsupportedOperationException();
      }
    };
  }
  private AbstractValueHolder<String> newAbstractValueHolder(final TimeUnit timeUnit, long creationTime, long expirationTime) {
    return new AbstractValueHolder<String>(-1, creationTime, expirationTime) {
      @Override
      protected TimeUnit nativeTimeUnit() {
        return timeUnit;
      }
      @Override
      public String value() {
        throw new UnsupportedOperationException();
      }
    };
  }
  private AbstractValueHolder<String> newAbstractValueHolder(final TimeUnit timeUnit, long creationTime, long expirationTime, long lastAccessTime) {
    final AbstractValueHolder<String> abstractValueHolder = new AbstractValueHolder<String>(-1, creationTime, expirationTime) {
      @Override
      protected TimeUnit nativeTimeUnit() {
        return timeUnit;
      }

      @Override
      public String value() {
        throw new UnsupportedOperationException();
      }
    };
    abstractValueHolder.setLastAccessTime(lastAccessTime, timeUnit);
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
