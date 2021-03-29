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
package org.gradoop.temporal.model.impl.operators.matching;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.TemporalGraphStatistics;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.BinningTemporalGraphStatisticsFactory;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.CypherTemporalPatternMatching;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopConfig;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Base class for all temporal pattern matching tests that read Temporal-GDL as input db.
 */
@RunWith(Parameterized.class)
public abstract class BaseTemporalPatternMatchingTest extends TemporalGradoopTestBase {

  /**
   * Name of the test.
   */
  protected final String testName;

  /**
   * The query as Temporal-GDL string.
   */
  protected final String queryGraph;

  /**
   * Expected graph variables (names).
   */
  protected final String[] expectedGraphVariables;

  /**
   * Expected graph collection as comma-separated Temporal-GDLs.
   */
  protected final String expectedCollection;

  private static TemporalGraphStatistics temporalGraphStatistics;

  @BeforeClass
  public static void setUp() throws Exception {
    ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
    TemporalGradoopConfig config = TemporalGradoopConfig.createConfig(environment);
    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(config);
    loader.initDatabaseFromFile(TEMPORAL_GDL_CITIBIKE_PATH);
    TemporalGraph temporalGraph = TemporalGraph.fromGraph(loader.getLogicalGraph());
    temporalGraph = config.getTemporalGraphFactory()
      .fromDataSets(
        temporalGraph.getVertices().map(vertexTransform),
        temporalGraph.getEdges().map(edgeTransform));
    temporalGraphStatistics = new BinningTemporalGraphStatisticsFactory()
      .fromGraph(temporalGraph);
  }

  /**
   * Initializes a test with a data graph.
   *
   * @param testName               name of the test
   * @param queryGraph             the query graph as Temporal-GDL string
   * @param expectedGraphVariables expected graph variables (names) as comma-separated string
   * @param expectedCollection     expected graph collection as comma-separated GDLs
   */
  public BaseTemporalPatternMatchingTest(String testName, String queryGraph, String expectedGraphVariables,
    String expectedCollection) {
    this.testName = testName;
    this.queryGraph = queryGraph;
    this.expectedGraphVariables = expectedGraphVariables.split(",");
    this.expectedCollection = expectedCollection;
  }

  /**
   * Given a graph collection, this method transforms it to a temporal graph collection.
   * {@code start} and {@code end} values are extracted from the edges (= "trips")
   * and used to set {@code valid_from}/{@code tx_from} and {@code valid_to}/{@code tx_to} values.
   *
   * @param graphCollection the collection to transform
   * @return the temporal representation
   */
  protected TemporalGraphCollection transformExpectedToTemporal(GraphCollection graphCollection) {
    //transform edges
    TemporalGraphCollection tgc = toTemporalGraphCollection(graphCollection);
    DataSet<TemporalEdge> newEdges = tgc.getEdges().map(edgeTransform);
    DataSet<TemporalVertex> newVertices = tgc.getVertices().map(vertexTransform);
    tgc = tgc.getFactory().fromDataSets(tgc.getGraphHeads(), newVertices, newEdges);
    return tgc;
  }

  /**
   * Yields a pattern matching implementation.
   *
   * @param queryGraph query graph as GDL string
   * @param stats graph statistics
   * @return a pattern matching implementation
   */
  public abstract CypherTemporalPatternMatching getImplementation(String queryGraph, TemporalGraphStatistics stats) throws Exception;

  @Test
  public void testPatternMatching() throws Exception {
    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(getConfig());

    loader.initDatabaseFromFile(TEMPORAL_GDL_CITIBIKE_PATH);

    TemporalGraph db = transformToTemporalGraph(loader.getLogicalGraph());

    loader.appendToDatabaseFromString(expectedCollection);

    TemporalGraphCollection result = getImplementation(queryGraph, temporalGraphStatistics)
      .execute(db);

    TemporalGraphCollection expectedByID = toTemporalGraphCollection(
      loader.getGraphCollectionByVariables(expectedGraphVariables));

    TemporalGraphCollection expectedByData = transformExpectedToTemporal(
      loader.getGraphCollectionByVariables(expectedGraphVariables));

    // element id equality
    collectAndAssertTrue(result.equalsByGraphElementIds(expectedByID));
    // graph element equality
    collectAndAssertTrue(result.equalsByGraphElementData(expectedByData));
  }
}
