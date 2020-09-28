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
package org.gradoop.temporal.model.impl.operators.matching;


import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.operators.matching.single.PatternMatching;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.model.impl.operators.matching.single.TemporalPatternMatching;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

/**
 * base class for all temporal pattern matching tests that read ASCII-data as input db.
 */
@RunWith(Parameterized.class)
public abstract class ASCIITemporalPatternMatchingTest extends TemporalGradoopTestBase {

  /**
   * name of the test
   */
  protected final String testName;

  /**
   * the string describing the path to the DB graph to query
   */
  protected final String dataGraphPath;

  /**
   * the query as GDL string
   */
  protected final String queryGraph;

  /**
   * expected graph variables (names) as comma-separated string
   */
  protected final String[] expectedGraphVariables;

  /**
   * expected graph collection as comma-separated GDLs
   */
  protected final String expectedCollection;


  /**
   * initializes a test with a data graph
   *
   * @param testName               name of the test
   * @param queryGraph             the query graph as GDL-string
   * @param dataGraphPath          path to data graph file
   * @param expectedGraphVariables expected graph variables (names) as comma-separated string
   * @param expectedCollection     expected graph collection as comma-separated GDLs
   */
  public ASCIITemporalPatternMatchingTest(String testName, String dataGraphPath, String queryGraph,
                                          String expectedGraphVariables, String expectedCollection) {
    this.testName = testName;
    this.dataGraphPath = dataGraphPath;
    this.queryGraph = queryGraph;
    this.expectedGraphVariables = expectedGraphVariables.split(",");
    this.expectedCollection = expectedCollection;
  }

  /**
   * Yields a pattern matching implementation
   *
   * @param queryGraph query graph as GDL string
   * @param attachData parameter of matching implementations
   * @return a pattern matching implementation
   */
  public abstract TemporalPatternMatching<
    TemporalGraphHead, TemporalGraph, TemporalGraphCollection> getImplementation(
    String queryGraph, boolean attachData);

  @Test
  public void testGraphElementIdEquality() throws Exception {
    FlinkAsciiGraphLoader loader = getLoader();

    TemporalGraph db = getTemporalGraphFromLoader(loader);

    loader.appendToDatabaseFromString(expectedCollection);


    TemporalGraphCollection result = getImplementation(queryGraph, true).execute(db);
    System.out.println(result.getGraphHeads().count());
    printResult(result);

    TemporalGraphCollection expectedByID = toTemporalGraphCollection(
      loader.getGraphCollectionByVariables(expectedGraphVariables));

    TemporalGraphCollection expectedByData = transformExpectedToTemporal(
      loader.getGraphCollectionByVariables(expectedGraphVariables));


    List<TemporalGraphHead> graphHeads = result.getGraphHeads().collect();
    System.out.println("expected: " + expectedCollection);
    printResult(result);
    //testGraphHeads(result);
    // exists variable mapping?
    for (TemporalGraphHead graphHead : graphHeads) {
      assertTrue(graphHead.hasProperty(PatternMatching.VARIABLE_MAPPING_KEY));
    }

    // element id equality
    collectAndAssertTrue(result.equalsByGraphElementIds(expectedByID));
    // graph element equality
    collectAndAssertTrue(result.equalsByGraphElementData(expectedByData));
    // correct times in graph head

  }


  private void printResult(TemporalGraphCollection gc) throws Exception {
    ArrayList<Integer> vertices = gc.getVertices().collect().stream()
      .map(v -> v.getPropertyValue("vertexId").getInt())
      .collect(Collectors.toCollection(ArrayList::new));
    ArrayList<Integer> edges = gc.getEdges().collect().stream()
      .map(v -> v.getPropertyValue("edgeId").getInt())
      .collect(Collectors.toCollection(ArrayList::new));

    System.out.println("Vertices " + vertices);
    System.out.println("Edges " + edges);
    System.out.println();
  }

  /**
   * In {@code testGraphElementEquality}, the {@code expected} variable might not be compatible
   * to the result graphs because EPGM graphs are transformed to TPGM graphs in
   * {@code getTemporalGraphFromLoader}. This method converts {@code expected} to the proper
   * format in order to make it comparable to {@code result}.
   *
   * @param graphCollectionByVariables the graph collection to transform to TPGM
   * @return transformed graph collection
   */
  protected abstract TemporalGraphCollection transformExpectedToTemporal(
    GraphCollection graphCollectionByVariables) throws Exception;

  /**
   * Creates a temporal graph from the input of a loader
   *
   * @param loader a loader
   * @return temporal graph from the input of the loader
   */
  protected abstract TemporalGraph getTemporalGraphFromLoader(FlinkAsciiGraphLoader loader) throws Exception;

  /**
   * creates a loader for test data
   *
   * @return loader for test data
   */
  protected abstract FlinkAsciiGraphLoader getLoader();
}
