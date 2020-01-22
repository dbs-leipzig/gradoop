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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.EdgeFactory;
import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.api.functions.TemporalPredicate;
import org.gradoop.temporal.model.impl.operators.snapshot.functions.ByTemporalPredicate;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.MIN_VALUE;
import static org.testng.AssertJUnit.assertArrayEquals;

/**
 * Test for the {@link ByTemporalPredicate} filter function.
 */
public class ByTemporalPredicateTest extends TemporalGradoopTestBase {

  /**
   * A temporal predicate used for testing.
   * Note that existing implementations of temporal predicates will be tested separately.
   */
  private final TemporalPredicate testPredicate = (from, to) -> from <= 2L && to > 5L;

  /**
   * A list of intervals to test. Those intervals should be accepted by the predicate.
   */
  private final List<Tuple2<Long, Long>> testIntervalsAccepted = Arrays.asList(
    Tuple2.of(MIN_VALUE, MAX_VALUE),
    Tuple2.of(1L, MAX_VALUE),
    Tuple2.of(2L, 6L),
    Tuple2.of(MIN_VALUE, 6L)
  );

  /**
   * A list of intervals to test. Those intervals should not be accepted by the predicate.
   */
  private final List<Tuple2<Long, Long>> testIntervalsOther = Arrays.asList(
    Tuple2.of(MIN_VALUE, 1L),
    Tuple2.of(2L, 4L),
    Tuple2.of(3L, MAX_VALUE),
    Tuple2.of(MIN_VALUE, 1L)
  );

  /**
   * Test the filter function on vertex tuples.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testForVertices() throws Exception {
    final VertexFactory<TemporalVertex> vertexFactory =
      getConfig().getTemporalGraphFactory().getVertexFactory();
    // Create vertex tuples for each accepted interval.
    List<TemporalVertex> tuplesAccepted = testIntervalsAccepted.stream()
      .map(i -> {
        TemporalVertex vertex = vertexFactory.createVertex();
        vertex.setValidTime(i);
        return vertex;
      })
      .collect(Collectors.toList());
    List<TemporalVertex> inputTuples = new ArrayList<>(tuplesAccepted);
    // Create vertex tuples for other intervals.
    testIntervalsOther.stream().map(i -> {
      TemporalVertex vertex = vertexFactory.createVertex();
      vertex.setValidTime(i);
      return vertex;
    })
      .forEach(inputTuples::add);
    // Apply the filter to the input.
    List<TemporalVertex> result = getExecutionEnvironment()
      .fromCollection(inputTuples, TypeInformation.of(TemporalVertex.class))
      .filter(new ByTemporalPredicate<>(testPredicate, TimeDimension.VALID_TIME))
      .collect();
    // Sort the result and expected results to allow for comparison.
    Comparator<TemporalVertex> comparator = Comparator.comparing(TemporalVertex::getId);
    result.sort(comparator);
    tuplesAccepted.sort(comparator);
    assertArrayEquals(tuplesAccepted.toArray(), result.toArray());
  }

  /**
   * Test the filter function on edge tuples.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testForEdges() throws Exception {
    final EdgeFactory<TemporalEdge> edgeFactory =
      getConfig().getTemporalGraphFactory().getEdgeFactory();
    // Create edge tuples for each accepted interval.
    List<TemporalEdge> tuplesAccepted = testIntervalsAccepted.stream()
      .map(i -> {
        TemporalEdge edge = edgeFactory.createEdge(GradoopId.get(), GradoopId.get());
        edge.setValidTime(i);
        return edge;
      })
      .collect(Collectors.toList());
    List<TemporalEdge> inputTuples = new ArrayList<>(tuplesAccepted);
    // Create edge tuples for other intervals.
    testIntervalsOther.stream()
      .map(i -> {
        TemporalEdge edge = edgeFactory.createEdge(GradoopId.get(), GradoopId.get());
        edge.setValidTime(i);
        return edge;
      })
      .forEach(inputTuples::add);
    // Apply the filter to the input.
    List<TemporalEdge> result = getExecutionEnvironment()
      .fromCollection(inputTuples, TypeInformation.of(TemporalEdge.class))
      .filter(new ByTemporalPredicate<>(testPredicate, TimeDimension.VALID_TIME))
      .collect();
    // Sort the result and expected results to allow for comparison.
    Comparator<TemporalEdge> comparator = Comparator.comparing(TemporalEdge::getId);
    result.sort(comparator);
    tuplesAccepted.sort(comparator);
    assertArrayEquals(tuplesAccepted.toArray(), result.toArray());
  }
}
