package org.ehcache.impl.internal.classes.commonslang;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * @author Henri Tremblay
 */
public class JavaVersionTest {

  private String specification;

  @Before
  public void before() {
    specification = System.getProperty("java.specification.version");
    System.setProperty("java.specification.version", "20");
  }

  @After
  public void after() {
    System.setProperty("java.specification.version", specification);
  }

  @Test
  public void javaRecent() {
    assertThat(JavaVersion.JAVA_RECENT.toString()).isEqualTo("20.0");
  }

  @Test
  public void getJavaVersion() {
    assertThat(JavaVersion.getJavaVersion(null)).isNull();
    assertThat(JavaVersion.getJavaVersion("0.9")).isEqualTo(JavaVersion.JAVA_0_9);
    assertThat(JavaVersion.getJavaVersion("1.1")).isEqualTo(JavaVersion.JAVA_1_1);
    assertThat(JavaVersion.getJavaVersion("1.2")).isEqualTo(JavaVersion.JAVA_1_2);
    assertThat(JavaVersion.getJavaVersion("1.3")).isEqualTo(JavaVersion.JAVA_1_3);
    assertThat(JavaVersion.getJavaVersion("1.4")).isEqualTo(JavaVersion.JAVA_1_4);
    assertThat(JavaVersion.getJavaVersion("1.5")).isEqualTo(JavaVersion.JAVA_1_5);
    assertThat(JavaVersion.getJavaVersion("1.6")).isEqualTo(JavaVersion.JAVA_1_6);
    assertThat(JavaVersion.getJavaVersion("1.7")).isEqualTo(JavaVersion.JAVA_1_7);
    assertThat(JavaVersion.getJavaVersion("1.8")).isEqualTo(JavaVersion.JAVA_1_8);
    assertThat(JavaVersion.getJavaVersion("9")).isEqualTo(JavaVersion.JAVA_9);
    assertThat(JavaVersion.getJavaVersion("10")).isEqualTo(JavaVersion.JAVA_RECENT);
  }

  @Test
  public void atLeast_true() {
    assertThat(JavaVersion.JAVA_1_8.atLeast(JavaVersion.JAVA_1_7)).isTrue();
    assertThat(JavaVersion.JAVA_9.atLeast(JavaVersion.JAVA_1_8)).isTrue();
    assertThat(JavaVersion.JAVA_9.atLeast(JavaVersion.JAVA_9)).isTrue();
    assertThat(JavaVersion.getJavaVersion("10").atLeast(JavaVersion.JAVA_9)).isTrue();
  }

  @Test
  public void atLeast_false() {
    assertThat(JavaVersion.JAVA_1_7.atLeast(JavaVersion.JAVA_1_8)).isFalse();
    assertThat(JavaVersion.JAVA_1_8.atLeast(JavaVersion.JAVA_9)).isFalse();
    assertThat(JavaVersion.JAVA_9.atLeast(JavaVersion.getJavaVersion("10"))).isFalse();
  }
}
