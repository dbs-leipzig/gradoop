/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.transformation;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.List;

public abstract class ApplyTransformationTest extends TransformationTest {

  @Test(expected = IllegalArgumentException.class)
  public void testMissingFunctions() {
    new ApplyTransformation(null, null, null);
  }

  @Test
  public void testIdEquality() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TEST_GRAPH);

    GraphCollection inputCollection = loader
      .getGraphCollectionByVariables("g0", "g1");

    List<GradoopId> expectedGraphHeadIds  = Lists.newArrayList();
    List<GradoopId> expectedVertexIds     = Lists.newArrayList();
    List<GradoopId> expectedEdgeIds       = Lists.newArrayList();

    inputCollection.getGraphHeads().map(new Id<>()).output(
      new LocalCollectionOutputFormat<>(expectedGraphHeadIds));
    inputCollection.getVertices().map(new Id<>()).output(
      new LocalCollectionOutputFormat<>(expectedVertexIds));
    inputCollection.getEdges().map(new Id<>()).output(
      new LocalCollectionOutputFormat<>(expectedEdgeIds));

    GraphCollection outputCollection = inputCollection
      .apply(new ApplyTransformation(
        TransformationTest::transformGraphHead,
        TransformationTest::transformVertex,
        TransformationTest::transformEdge));

    List<GradoopId> resultGraphHeadIds = Lists.newArrayList();
    List<GradoopId> resultVertexIds    = Lists.newArrayList();
    List<GradoopId> resultEdgeIds      = Lists.newArrayList();

    outputCollection.getGraphHeads().map(new Id<>()).output(
      new LocalCollectionOutputFormat<>(resultGraphHeadIds));
    outputCollection.getVertices().map(new Id<>()).output(
      new LocalCollectionOutputFormat<>(resultVertexIds));
    outputCollection.getEdges().map(new Id<>()).output(
      new LocalCollectionOutputFormat<>(resultEdgeIds));

    getExecutionEnvironment().execute();

    GradoopTestUtils.validateIdEquality(
      expectedGraphHeadIds,
      resultGraphHeadIds);
    GradoopTestUtils.validateIdEquality(
      expectedVertexIds,
      resultVertexIds);
    GradoopTestUtils.validateIdEquality(
      expectedEdgeIds,
      resultEdgeIds);
  }

  @Test
  public void testDataEquality() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TEST_GRAPH);

    GraphCollection inputCollection = loader
      .getGraphCollectionByVariables("g0", "g1");

    GraphCollection expectedCollection = loader
      .getGraphCollectionByVariables("g01", "g11");

    GraphCollection outputCollection = inputCollection
      .apply(new ApplyTransformation(
        TransformationTest::transformGraphHead,
        TransformationTest::transformVertex,
        TransformationTest::transformEdge));

    collectAndAssertTrue(
      outputCollection.equalsByGraphData(expectedCollection));
  }

  @Test
  public void testGraphHeadOnlyTransformation() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TEST_GRAPH);

    GraphCollection inputCollection =
      loader.getGraphCollectionByVariables("g0", "g1");

    GraphCollection expectedCollection =
      loader.getGraphCollectionByVariables("g02", "g12");

    GraphCollection outputCollection = inputCollection
      .apply(new ApplyTransformation(TransformationTest::transformGraphHead, null, null));

    collectAndAssertTrue(
      outputCollection.equalsByGraphData(expectedCollection));
  }

  @Test
  public void testVertexOnlyTransformation() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TEST_GRAPH);

    GraphCollection inputCollection = loader
      .getGraphCollectionByVariables("g0", "g1");

    GraphCollection expectedCollection = loader
      .getGraphCollectionByVariables("g03", "g13");

    GraphCollection outputCollection = inputCollection
      .apply(new ApplyTransformation(null, TransformationTest::transformVertex, null));

    collectAndAssertTrue(
      outputCollection.equalsByGraphData(expectedCollection));
  }

  @Test
  public void testEdgeOnlyTransformation() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(TEST_GRAPH);

    GraphCollection inputCollection = loader
      .getGraphCollectionByVariables("g0", "g1");

    GraphCollection expectedCollection = loader
      .getGraphCollectionByVariables("g04", "g14");

    GraphCollection outputCollection = inputCollection.apply(
        new ApplyTransformation(null, null, TransformationTest::transformEdge));

    collectAndAssertTrue(
      outputCollection.equalsByGraphData(expectedCollection));
  }
}
