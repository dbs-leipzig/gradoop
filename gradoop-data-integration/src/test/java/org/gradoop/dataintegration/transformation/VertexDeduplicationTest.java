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
package org.gradoop.dataintegration.transformation;

import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test for the {@link VertexDeduplication} operator.
 */
public class VertexDeduplicationTest extends GradoopFlinkTestBase {

  /**
   * Run the operator on a test graph.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testWithGraph() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
      "(va1:a {key: 1L})-[:e {att: 1L}]->(vb1:a {key: 1.0d})-[:e {att: 2L}]->(va2:a {key: 1L})" +
      "(vc1:a {key: 1L, key2: \"a\"})-[:e {att: 3L}]->(va1)-[:e {att: 4L}]->(b1:b {key: 1L})" +
      "(vc2:a {key: 1L, key2: \"a\"})" +
      "(vb2:a {key: 1.0d})-[:e {att: 5L}]->(vb3:a {key: 1.0d})(vd1:a {key: 1L, key2: \"b\"})" +
      "]" +
      "expected [" +
      "(da:a {key: 1L})-[:e {att: 1L}]->(db:a {key: 1.0d})-[:e {att: 2L}]->(da)" +
      "(dc:a {key: 1L, key2: \"a\"})-[:e {att: 3L}]->(da)-[:e {att: 4L}]->(b:b {key: 1L})" +
      "(db)-[:e {att: 5L}]->(db)(dd:a {key: 1L, key2: \"b\"})" +
      "]");
    LogicalGraph result = loader.getLogicalGraphByVariable("input")
      .callForGraph(new VertexDeduplication<>("a", Arrays.asList("key", "key2")));
    collectAndAssertTrue(result.equalsByData(loader.getLogicalGraphByVariable("expected")));
  }

  /**
   * Run the operator on a graph not containing the label.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testWithUnknownLabel() throws Exception {
    LogicalGraph socialGraph = getSocialNetworkLoader().getLogicalGraph();
    LogicalGraph result = socialGraph
      .callForGraph(new VertexDeduplication<>("NotInTheGraph", Arrays.asList("a", "b")));
    collectAndAssertTrue(socialGraph.equalsByData(result));
  }

  /**
   * Run the operator on a graph with the property not set.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testWithUnknownProperty() throws Exception {
    LogicalGraph socialGraph = getSocialNetworkLoader().getLogicalGraph();
    List<EPGMVertex> vertices = socialGraph
      .callForGraph(new VertexDeduplication<>("Person", Collections.singletonList("notSet")))
      .getVerticesByLabel("Person").collect();
    assertEquals(1, vertices.size());
  }
}
