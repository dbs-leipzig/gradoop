/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayoutFactory;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayoutFactory;
import org.gradoop.flink.model.impl.functions.bool.False;
import org.gradoop.flink.model.impl.layouts.gve.GVECollectionLayoutFactory;
import org.gradoop.flink.model.impl.layouts.gve.GVEGraphLayoutFactory;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.junit.ClassRule;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Base class for Flink-based unit tests with the same cluster.
 */
public abstract class GradoopFlinkTestBase {

  private static final int DEFAULT_PARALLELISM = 4;

  private static final long TASKMANAGER_MEMORY_SIZE_MB = 512;

  @ClassRule
  public static MiniClusterWithClientResource miniClusterResource = getMiniCluster();

  /**
   * Flink Execution Environment
   */
  private ExecutionEnvironment env;

  /**
   * Gradoop Flink configuration
   */
  private GradoopFlinkConfig config;

  /**
   * The factory to create a logical graph layout.
   */
  private LogicalGraphLayoutFactory<EPGMGraphHead, EPGMVertex, EPGMEdge> graphLayoutFactory;

  /**
   * The factory to create a graph collection layout.
   */
  private GraphCollectionLayoutFactory<EPGMGraphHead, EPGMVertex, EPGMEdge> collectionLayoutFactory;

  /**
   * Creates a new instance of {@link GradoopFlinkTestBase}.
   */
  public GradoopFlinkTestBase() {
    this.env = ExecutionEnvironment.getExecutionEnvironment();
    this.graphLayoutFactory = new GVEGraphLayoutFactory();
    this.collectionLayoutFactory = new GVECollectionLayoutFactory();
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
   * Returns the configuration for the test
   *
   * @return Gradoop Flink configuration
   */
  protected GradoopFlinkConfig getConfig() {
    if (config == null) {
      setConfig(GradoopFlinkConfig.createConfig(getExecutionEnvironment(),
        graphLayoutFactory,
        collectionLayoutFactory));
    }
    return config;
  }

  /**
   * Sets the default configuration for the test
   */
  protected void setConfig(GradoopFlinkConfig config) {
    this.config = config;
  }

  protected void setCollectionLayoutFactory(
    GraphCollectionLayoutFactory<EPGMGraphHead, EPGMVertex, EPGMEdge> collectionLayoutFactory) {
    this.collectionLayoutFactory = collectionLayoutFactory;
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
   */
  private static MiniClusterWithClientResource getMiniCluster() {
    Configuration config = new Configuration();
    config.setLong("taskmanager.memory.size", TASKMANAGER_MEMORY_SIZE_MB);

    return new MiniClusterWithClientResource(
      new MiniClusterResourceConfiguration.Builder()
        .setNumberTaskManagers(1)
        .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
        .setConfiguration(config).build());
  }

  //----------------------------------------------------------------------------
  // Data generation
  //----------------------------------------------------------------------------

  protected FlinkAsciiGraphLoader getLoaderFromString(String asciiString) {
    FlinkAsciiGraphLoader loader = getNewLoader();
    loader.initDatabaseFromString(asciiString);
    return loader;
  }

  protected FlinkAsciiGraphLoader getLoaderFromFile(String fileName) throws IOException {
    FlinkAsciiGraphLoader loader = getNewLoader();

    loader.initDatabaseFromFile(fileName);
    return loader;
  }

  protected FlinkAsciiGraphLoader getLoaderFromStream(InputStream inputStream) throws IOException {
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
    return new FlinkAsciiGraphLoader(getConfig());
  }

  //----------------------------------------------------------------------------
  // Test helper
  //----------------------------------------------------------------------------

  protected void collectAndAssertTrue(DataSet<Boolean> result) throws Exception {
    assertTrue("expected true", result.collect().get(0));
  }

  protected void collectAndAssertFalse(DataSet<Boolean> result) throws Exception {
    assertFalse("expected false", result.collect().get(0));
  }

  protected <T extends Element> DataSet<T> getEmptyDataSet(T dummy) {
    return getExecutionEnvironment()
      .fromElements(dummy)
      .filter(new False<>());
  }

  /**
   * Returns the encoded file path to a resource.
   *
   * @param  relPath the relative path to the resource
   * @return encoded file path
   */
  protected String getFilePath(String relPath) throws UnsupportedEncodingException {
    return URLDecoder.decode(
      getClass().getResource(relPath).getFile(), StandardCharsets.UTF_8.name());
  }
}
