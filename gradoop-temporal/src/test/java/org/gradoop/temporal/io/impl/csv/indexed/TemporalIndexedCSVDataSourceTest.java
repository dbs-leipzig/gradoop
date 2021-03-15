/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.io.impl.csv.indexed;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;

/**
 * Test class to test {@link TemporalIndexedCSVDataSource}.
 */
public class TemporalIndexedCSVDataSourceTest extends TemporalGradoopTestBase {

  /**
   * Temporal graph to test
   */
  private TemporalGraph testGraph;

  /**
   * Temporal graph collection to test
   */
  private TemporalGraphCollection testGraphCollection;

  /**
   * Temporary test folder to write the test graph.
   */
  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();

  /**
   * Temporary test folder to write the test graph.
   */
  @Rule
  public TemporaryFolder testCollectionFolder = new TemporaryFolder();

  /**
   * Creates a test temporal graph from the social network loader.
   *
   * @throws Exception if loading the graph fails
   */
  @Before
  public void setUp() throws Exception {
    testGraph = toTemporalGraph(getSocialNetworkLoader().getLogicalGraph())
      .transformGraphHead((current, trans) -> {
        current.setProperty("testGraphHeadProperty", PropertyValue.create(1L));
        return current;
      });

    testGraphCollection = toTemporalGraphCollection(getSocialNetworkLoader().getGraphCollection());
  }

  /**
   * Tests the function {@link TemporalIndexedCSVDataSource#getTemporalGraph()}.
   *
   * @throws Exception if loading the graph fails
   */
  @Test
  public void testGetTemporalGraph() throws Exception {
    new TemporalIndexedCSVDataSink(testFolder.getRoot().getPath(), getConfig()).write(testGraph);
    getExecutionEnvironment().execute();
    TemporalIndexedCSVDataSource source = new TemporalIndexedCSVDataSource(testFolder.getRoot().getPath(),
      getConfig());
    collectAndAssertTrue(source.getTemporalGraph().equalsByData(testGraph));
  }

  /**
   * Tests the function {@link TemporalIndexedCSVDataSource#getTemporalGraphCollection()}.
   *
   * @throws Exception if loading the collection fails
   */
  @Test
  public void testGetTemporalGraphCollection() throws Exception {
    new TemporalIndexedCSVDataSink(testFolder.getRoot().getPath(), getConfig()).write(testGraphCollection);
    getExecutionEnvironment().execute();
    TemporalIndexedCSVDataSource source = new TemporalIndexedCSVDataSource(testFolder.getRoot().getPath(),
      getConfig());
    collectAndAssertTrue(source.getTemporalGraphCollection().equalsByGraphData(testGraphCollection));
  }

  /**
   * Tests the function {@link TemporalIndexedCSVDataSource#getConfig()}.
   */
  @Test
  public void testGetConfig() {
    TemporalIndexedCSVDataSource source = new TemporalIndexedCSVDataSource(testFolder.getRoot().getPath(),
      getConfig());
    assertEquals(getConfig(), source.getConfig());
  }
}
