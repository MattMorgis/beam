/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.fnexecution.environment;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ProcessManager}. */
@RunWith(JUnit4.class)
public class ProcessManagerTest {

  @Test
  public void testRunSimpleCommand() throws IOException {
    ProcessManager processManager = ProcessManager.create();
    processManager.startProcess("1", "bash", Collections.emptyList());
    processManager.stopProcess("1");
    processManager.startProcess("2", "bash", Arrays.asList("-c", "ls"));
    processManager.stopProcess("2");
    processManager.startProcess("1", "bash", Arrays.asList("-c", "ls", "-l", "-a"));
    processManager.stopProcess("1");
  }

  @Test
  public void testRunInvalidExecutable() throws IOException {
    ProcessManager processManager = ProcessManager.create();
    try {
      processManager.startProcess("1", "asfasfls", Collections.emptyList());
      fail();
    } catch (IOException e) {
      assertThat(e.getMessage(), containsString("Cannot run program \"asfasfls\""));
    }
  }

  @Test
  public void testDuplicateId() throws IOException {
    ProcessManager processManager = ProcessManager.create();
    processManager.startProcess("1", "bash", Arrays.asList("-c", "ls"));
    try {
      processManager.startProcess("1", "bash", Arrays.asList("-c", "ls"));
      fail();
    } catch (IllegalStateException e) {
      // this is what we want
    } finally {
      processManager.stopProcess("1");
    }
  }

  @Test
  public void testLivenessCheck() throws IOException {
    ProcessManager processManager = ProcessManager.create();
    ProcessManager.RunningProcess process =
        processManager.startProcess("1", "bash", Arrays.asList("-c", "sleep", "1000"));
    process.isAliveOrThrow();
    processManager.stopProcess("1");
    try {
      process.isAliveOrThrow();
      fail();
    } catch (IllegalStateException e) {
      // this is what we want
    }
  }

  @Test
  public void testEnvironmentVariables() throws IOException, InterruptedException {
    ProcessManager processManager = ProcessManager.create();
    ProcessManager.RunningProcess process =
        processManager.startProcess(
            "1",
            "bash",
            Arrays.asList("-c", "sleep $PARAM"),
            Collections.singletonMap("PARAM", "-h"));
    for (int i = 0; i < 10 && process.getUnderlyingProcess().isAlive(); i++) {
      Thread.sleep(100);
    }
    assertThat(process.getUnderlyingProcess().exitValue(), is(1));
    processManager.stopProcess("1");
  }

  @Test
  public void testRedirectOutput() throws IOException, InterruptedException {
    File outputFile = File.createTempFile("beam-redirect-output-", "");
    outputFile.deleteOnExit();
    ProcessManager processManager = ProcessManager.create();
    ProcessManager.RunningProcess process =
        processManager.startProcess(
            "1",
            "bash",
            Arrays.asList("-c", "echo 'testing123'"),
            Collections.emptyMap(),
            outputFile);
    for (int i = 0; i < 10 && process.getUnderlyingProcess().isAlive(); i++) {
      Thread.sleep(100);
    }
    processManager.stopProcess("1");
    byte[] output = Files.readAllBytes(outputFile.toPath());
    assertNotNull(output);
    String outputStr = new String(output, Charset.defaultCharset());
    assertThat(outputStr, containsString("testing123"));
  }

  @Test
  public void testInheritIO() throws IOException, InterruptedException {
    final PrintStream oldOut = System.out;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream newOut = new PrintStream(baos);
    try {
      System.setOut(newOut);
      ProcessManager processManager = ProcessManager.create();
      ProcessManager.RunningProcess process =
          processManager.startProcess(
              "1",
              "bash",
              Arrays.asList("-c", "echo 'testing123' 1>&2;"),
              Collections.emptyMap(),
              ProcessManager.INHERIT_IO_FILE);
      for (int i = 0; i < 10 && process.getUnderlyingProcess().isAlive(); i++) {
        Thread.sleep(100);
      }
      processManager.stopProcess("1");
    } finally {
      System.setOut(oldOut);
    }
    // TODO: this doesn't work as inherit IO bypasses System.out/err
    // the output instead appears in the console
    // String outputStr = new String(baos.toByteArray(), Charset.defaultCharset());
    // assertThat(outputStr, containsString("testing123"));
    assertFalse(ProcessManager.INHERIT_IO_FILE.exists());
  }
}
