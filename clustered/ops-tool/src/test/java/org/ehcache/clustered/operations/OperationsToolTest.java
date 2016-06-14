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
package org.ehcache.clustered.operations;

import java.io.IOException;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import org.junit.Test;

public class OperationsToolTest {

  @Test
  public void testHelp() throws IOException {
    assertThat(run("--help"), is(1));
  }

  @Test
  public void testList() throws IOException {
    assertThat(run("--cluster http://example.com:1234/watman list"), is(0));
  }

  @Test
  public void testListWithoutCluster() throws IOException {
    assertThat(run("list"), is(1));
  }

  @Test
  public void testCreate() {
    assertThat(run("create --config file.xml"), is(0));
  }

  @Test
  public void testDryRunCreate() {
    assertThat(run("--dry-run create --config file.xml"), is(0));
  }

  @Test
  public void testOverridenCreate() {
    assertThat(run("--cluster http://example.com:1234/watman create --config file.xml"), is(0));
  }

  @Test
  public void testCreateWithMissingFile() {
    assertThat(run("create"), is(1));
  }

  @Test
  public void testCreateWithIllegalURI() {
    assertThat(run("--cluster ### create --config file.xml"), is(1));
  }

  @Test
  public void testDestroy() {
    assertThat(run("destroy --config file.xml"), is(0));
  }

  @Test
  public void testDryRunDestroy() {
    assertThat(run("--dry-run destroy --config file.xml"), is(0));
  }

  @Test
  public void testOverridenDestroy() {
    assertThat(run("--cluster http://example.com:1234/watman destroy --config file.xml"), is(0));
  }

  @Test
  public void testNonMatchingDestroy() {
    assertThat(run("destroy --config file.xml --match false"), is(0));
  }

  @Test
  public void testDestroyWithMissingFile() {
    assertThat(run("destroy"), is(1));
  }

  @Test
  public void testDestroyWithIllegalURI() {
    assertThat(run("--cluster ### destroy --config file.xml"), is(1));
  }

  @Test
  public void testUpdate() {
    assertThat(run("update --config file.xml"), is(0));
  }

  @Test
  public void testDryRunUpdate() {
    assertThat(run("--dry-run update --config file.xml"), is(0));
  }

  @Test
  public void testOverridenUpdate() {
    assertThat(run("--cluster http://example.com:1234/watman update --config file.xml"), is(0));
  }

  @Test
  public void testUpdateWithDeletions() {
    assertThat(run("update --config file.xml --allow-destroy"), is(0));
  }

  @Test
  public void testUpdateWithMutations() {
    assertThat(run("update --config file.xml --allow-mutation"), is(0));
  }

  @Test
  public void testUpdateWithMissingFile() {
    assertThat(run("update"), is(1));
  }

  @Test
  public void testUpdateWithIllegalURI() {
    assertThat(run("--cluster ### update --config file.xml"), is(1));
  }

  public static int run(String command) {
    return OperationsTool.innerMain(command.split("\\s+"));
  }
}
