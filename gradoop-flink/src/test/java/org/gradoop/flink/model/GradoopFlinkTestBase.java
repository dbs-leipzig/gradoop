/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.test.util.TestEnvironment;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Base class for Flink-based unit tests with the same cluster.
 */
public abstract class GradoopFlinkTestBase {

  protected static final int DEFAULT_PARALLELISM = 4;

  protected static final long TASKMANAGER_MEMORY_SIZE_MB = 256;

  protected static ForkableFlinkMiniCluster CLUSTER = null;

  /**
   * Flink Execution Environment
   */
  private ExecutionEnvironment env;

  /**
   * Gradoop Flink configuration
   */
  protected GradoopFlinkConfig config;

  public GradoopFlinkTestBase() {
    TestEnvironment testEnv = new TestEnvironment(CLUSTER, DEFAULT_PARALLELISM);
    // makes ExecutionEnvironment.getExecutionEnvironment() return this instance
    testEnv.setAsContext();
    this.env = testEnv;
    this.config = GradoopFlinkConfig.createConfig(env);
  }

  /**
   * Returns the execution environment for the tests
   *
   * @return Flink execution environment
   */
  protected ExecutionEnvironment getExecutionEnvironment() {
    return env;
  }

  /**
   * Returns the default configuration for the test
   *
   * @return Gradoop Flink configuration
   */
  protected GradoopFlinkConfig getConfig() {
    return config;
  }

  //----------------------------------------------------------------------------
  // Cluster related
  //----------------------------------------------------------------------------

  /**
   * Custom test cluster start routine,
   * workaround to set TASK_MANAGER_MEMORY_SIZE.
   *
   * TODO: remove, when future issue is fixed
   * {@see http://mail-archives.apache.org/mod_mbox/flink-dev/201511.mbox/%3CCAC27z=PmPMeaiNkrkoxNFzoR26BOOMaVMghkh1KLJFW4oxmUmw@mail.gmail.com%3E}
   *
   * @throws Exception
   */
  @BeforeClass
  public static void setup() throws Exception {
    File logDir = File.createTempFile("TestBaseUtils-logdir", null);
    Assert.assertTrue("Unable to delete temp file", logDir.delete());
    Assert.assertTrue("Unable to create temp directory", logDir.mkdir());

    Files.createFile((new File(logDir, "jobmanager.out")).toPath());
    Path logFile =  Files
      .createFile((new File(logDir, "jobmanager.log")).toPath());

    Configuration config = new Configuration();

    config.setInteger("local.number-taskmanager", 1);
    config.setInteger("taskmanager.numberOfTaskSlots", DEFAULT_PARALLELISM);
    config.setBoolean("local.start-webserver", false);
    config.setLong("taskmanager.memory.size", TASKMANAGER_MEMORY_SIZE_MB);
    config.setBoolean("fs.overwrite-files", true);
    config.setString("akka.ask.timeout", "1000s");
    config.setString("akka.startup-timeout", "60 s");
    config.setInteger("jobmanager.web.port", 8081);
    config.setString("jobmanager.web.log.path", logFile.toString());
    CLUSTER =
      new ForkableFlinkMiniCluster(config, true);
    CLUSTER.start();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TestBaseUtils.stopCluster(CLUSTER, new FiniteDuration(1000, TimeUnit.SECONDS));
  }

  //----------------------------------------------------------------------------
  // Data generation
  //----------------------------------------------------------------------------

  protected FlinkAsciiGraphLoader getLoaderFromString(String asciiString) {
    FlinkAsciiGraphLoader
      loader = getNewLoader();
    loader.initDatabaseFromString(asciiString);
    return loader;
  }

  protected FlinkAsciiGraphLoader
  getLoaderFromFile(
    String fileName) throws IOException {
    FlinkAsciiGraphLoader
      loader = getNewLoader();

    loader.initDatabaseFromFile(fileName);
    return loader;
  }

  protected FlinkAsciiGraphLoader getLoaderFromStream(InputStream inputStream)
    throws IOException {
    FlinkAsciiGraphLoader loader = getNewLoader();

    loader.initDatabaseFromStream(inputStream);
    return loader;
  }

  /**
   * Creates a social network as a basis for tests.
   * <p/>
   * An image of the network can be found in
   * gradoop/dev-support/social-network.pdf
   *
   * @return graph store containing a simple social network for tests.
   */
  protected FlinkAsciiGraphLoader getSocialNetworkLoader() throws IOException {
    InputStream inputStream = getClass()
      .getResourceAsStream(GradoopTestUtils.SOCIAL_NETWORK_GDL_FILE);
    return getLoaderFromStream(inputStream);
  }

  /**
   * Returns an uninitialized loader with the test config.
   *
   * @return uninitialized Flink Ascii graph loader
   */
  private FlinkAsciiGraphLoader getNewLoader() {
    return new FlinkAsciiGraphLoader(config);
  }

  //----------------------------------------------------------------------------
  // Test helper
  //----------------------------------------------------------------------------

  protected void collectAndAssertTrue(DataSet<Boolean> result) throws
    Exception {
    assertTrue("expected true", result.collect().get(0));
  }

  protected void collectAndAssertFalse(DataSet<Boolean> result) throws
    Exception {
    assertFalse("expected false", result.collect().get(0));
  }
}
