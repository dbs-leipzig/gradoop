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
package org.gradoop.temporal.io.impl.csv;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.temporal.io.api.TemporalDataSink;
import org.gradoop.temporal.io.api.TemporalDataSource;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * A test for the temporal CSV data source and sink.
 */
public class TemporalCSVSourceAndSinkTest extends TemporalGradoopTestBase {

  /**
   * Temporal graph to test
   */
  private TemporalGraph testGraph;

  /**
   * Temporary test folder to write the test graph.
   */
  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();

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
  }

  /**
   * Test the {@link TemporalGraph#writeTo(TemporalDataSink)} method as well as the
   * {@link TemporalCSVDataSource}.
   *
   * @throws Exception in case of failure
   */
  @Test
  public void testWriteTo() throws Exception {
    String tempFolderPath = testFolder.newFolder().getPath();

    testGraph.writeTo(new TemporalCSVDataSink(tempFolderPath, getConfig()));
    getExecutionEnvironment().execute();

    TemporalDataSource dataSource = new TemporalCSVDataSource(tempFolderPath, getConfig());

    collectAndAssertTrue(dataSource.getTemporalGraph().equalsByData(testGraph));
  }

  /**
   * Test the {@link TemporalGraph#writeTo(TemporalDataSink, boolean)} method as well as the
   * {@link TemporalCSVDataSource}.
   *
   * @throws Exception in case of failure
   */
  @Test
  public void testWriteToOverwrite() throws Exception {
    String tempFolderPath = testFolder.newFolder().getPath();

    testGraph.writeTo(new TemporalCSVDataSink(tempFolderPath, getConfig()));
    getExecutionEnvironment().execute();

    testGraph.writeTo(new TemporalCSVDataSink(tempFolderPath, getConfig()), true);
    getExecutionEnvironment().execute();

    TemporalDataSource dataSource = new TemporalCSVDataSource(tempFolderPath, getConfig());

    collectAndAssertTrue(dataSource.getTemporalGraph().equalsByData(testGraph));
  }
}
