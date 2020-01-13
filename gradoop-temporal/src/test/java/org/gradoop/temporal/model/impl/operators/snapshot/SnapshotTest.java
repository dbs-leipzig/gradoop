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
package org.gradoop.temporal.model.impl.operators.snapshot;

import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.EPGMElement;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.api.functions.TemporalPredicate;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.functions.predicates.All;
import org.gradoop.temporal.model.impl.functions.predicates.AsOf;
import org.gradoop.temporal.model.impl.functions.predicates.Between;
import org.gradoop.temporal.model.impl.functions.predicates.ContainedIn;
import org.gradoop.temporal.model.impl.functions.predicates.CreatedIn;
import org.gradoop.temporal.model.impl.functions.predicates.DeletedIn;
import org.gradoop.temporal.model.impl.functions.predicates.FromTo;
import org.gradoop.temporal.model.impl.functions.predicates.ValidDuring;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.testng.AssertJUnit;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Test for the snapshot operator for temporal graphs.
 */
public class SnapshotTest extends TemporalGradoopTestBase {

  /**
   * Run the test. Calls the snapshot operator using a predicate and compares results with the expected
   * result graph.
   *
   * @param predicate The {@link TemporalPredicate} to create the {@link Snapshot} operator with.
   * @param dimension The {@link TimeDimension} to create the {@link Snapshot} operator with.
   * @param expectedVertices An array of strings specifying the expected vertices.
   * @param expectedEdges An array of string specifying the expected edges.
   *
   * @throws Exception when the Execution in Flink fails.
   */
  @Test(dataProvider = "parameters")
  public void runTest(TemporalPredicate predicate, TimeDimension dimension, String[] expectedVertices,
                      String[] expectedEdges) throws Exception {
    TemporalGraph inputGraph = getTestGraphWithValues();

    Collection<TemporalVertex> resultVertices = new HashSet<>();
    Collection<TemporalEdge> resultEdges = new HashSet<>();

    TemporalGraph resultGraph = inputGraph.callForGraph(new Snapshot(predicate, dimension));

    resultGraph.getVertices().output(new LocalCollectionOutputFormat<>(resultVertices));
    resultGraph.getEdges().output(new LocalCollectionOutputFormat<>(resultEdges));

    getConfig().getExecutionEnvironment().execute();

    List<String> resultVertexLabels =
      resultVertices.stream().map(EPGMElement::getLabel).collect(Collectors.toList());
    List<String> resultEdgeLabels =
      resultEdges.stream().map(EPGMElement::getLabel).collect(Collectors.toList());

    assertEquals(expectedVertices.length, resultVertices.size());
    assertEquals(expectedEdges.length, resultEdges.size());

    Arrays.stream(expectedVertices).map(resultVertexLabels::contains).forEach(AssertJUnit::assertTrue);
    Arrays.stream(expectedEdges).map(resultEdgeLabels::contains).forEach(AssertJUnit::assertTrue);
  }

  /**
   * Parameters for this parametrized test.
   *
   * @return An array containing arrays in the form of {@code {predicate, dimension, inputGraph,
   * extraGraphData}}.
   */
  @DataProvider
  public static Object[][] parameters() {
    return new Object[][] {
      {new All(), TimeDimension.VALID_TIME, new String[] {V1, V2, V3, V4, V5}, new String[] {E1, E2, E3, E4, E5}},
      {new AsOf(3L), TimeDimension.VALID_TIME, new String[] {V1, V2, V3, V5}, new String[] {E1}},
      {new Between(2L, 3L), TimeDimension.VALID_TIME, new String[] {V1, V2, V3, V5}, new String[] {E1}},
      {new ContainedIn(0L, 5L), TimeDimension.VALID_TIME, new String[] {V4}, new String[] {E3, E5}},
      {new CreatedIn(2L, 5L), TimeDimension.VALID_TIME, new String[] {V3, V4}, new String[] {E4, E5}},
      {new DeletedIn(6L, 10L), TimeDimension.VALID_TIME, new String[] {V3, V5}, new String[] {E2, E4}},
      {new FromTo(1L, 3L), TimeDimension.VALID_TIME, new String[] {V1, V2, V5}, new String[] {E1}},
      {new ValidDuring(3L, 8L), TimeDimension.VALID_TIME, new String[] {V1, V2, V3, V5}, new String[] {E1}},
      {new All(), TimeDimension.TRANSACTION_TIME, new String[] {V1, V2, V3, V4, V5},
        new String[] {E1, E2, E3, E4, E5}},
      {new AsOf(3L), TimeDimension.TRANSACTION_TIME, new String[] {V1, V2, V3, V4, V5},
        new String[] {E1, E3, E5}},
      {new Between(4L, 8L), TimeDimension.TRANSACTION_TIME, new String[] {V1, V2, V3, V4, V5},
        new String[] {E1, E4, E5}},
      {new ContainedIn(2L, 8L), TimeDimension.TRANSACTION_TIME, new String[] {V4, V5}, new String[] {E3, E4, E5}},
      {new CreatedIn(0L, 5L), TimeDimension.TRANSACTION_TIME, new String[] {V2, V3, V4, V5},
        new String[] {E1, E2, E3, E5}},
      {new DeletedIn(5L, 10L), TimeDimension.TRANSACTION_TIME, new String[] {V3, V4, V5}, new String[] {E4, E5}},
      {new FromTo(2L, 6L), TimeDimension.TRANSACTION_TIME, new String[] {V1, V2, V3, V4, V5},
        new String[] {E1, E3, E5}},
      {new ValidDuring(1L, 6L), TimeDimension.TRANSACTION_TIME, new String[] {V1, V2, V3}, new String[] {E1}}
    };
  }
}
