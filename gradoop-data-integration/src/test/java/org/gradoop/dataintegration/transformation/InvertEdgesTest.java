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
package org.gradoop.dataintegration.transformation;

import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.ByLabel;
import org.gradoop.flink.model.impl.functions.filters.Or;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

/**
 * Tests for the invert edge operator.
 */
public class InvertEdgesTest extends GradoopFlinkTestBase {

  /**
   * Test to ensure that the first label is never null.
   */
  @Test(expected = NullPointerException.class)
  public void firstNullArgumentTest() {
    new InvertEdges(null, "foo");
  }

  /**
   * Test to ensure that the second label is never null.
   */
  @Test(expected = NullPointerException.class)
  public void secondNullArgumentTest() {
    new InvertEdges("foo", null);
  }

  /**
   * Test whether edges are inverted correctly.
   *
   * @throws Exception If test data can't be loaded.
   */
  @Test
  public void testInvert() throws Exception {
    final String toInvertLabel = "hasInterest";
    final String invertedLabel = "foobar";
    LogicalGraph social = getSocialNetworkLoader().getLogicalGraph();

    InvertEdges invertEdges = new InvertEdges(toInvertLabel, invertedLabel);
    LogicalGraph invertedEdgeGraph = social.transformEdges(invertEdges);

    long edgesBefore = social.getEdges().count();
    long edgesToChange = social.getEdges().filter(new ByLabel<>(toInvertLabel)).count();
    long edgesAfter = invertedEdgeGraph.getEdges().count();
    Assert.assertEquals(edgesToChange, 4); // we have 4 "hasInterest" edges
    Assert.assertEquals(edgesBefore, edgesAfter); // ensures no new edges are created

    long oldEdgeCount = invertedEdgeGraph.getEdges().filter(new ByLabel<>(toInvertLabel)).count();
    Assert.assertEquals(oldEdgeCount, 0); // no edges with the old label should exist

    /*
     * We now have to check whether all of these hasInterest edges are inverted.
     * (eve)-[:hasInterest]->(databases)
     * (alice)-[:hasInterest]->(databases)
     * (frank)-[:hasInterest]->(hadoop)
     * (dave)-[:hasInterest]->(hadoop)
     */
    List<Vertex> vertices = new ArrayList<>();
    invertedEdgeGraph.getVertices()
        .filter(new Or<>(new ByLabel<>("Person"), new ByLabel<>("Tag")))
        .output(new LocalCollectionOutputFormat<>(vertices));

    List<Edge> newEdges = new ArrayList<>();
    invertedEdgeGraph
        .getEdgesByLabel(invertedLabel)
        .output(new LocalCollectionOutputFormat<>(newEdges));

    getConfig().getExecutionEnvironment().execute();

    Map<GradoopId, String> idMap = new HashMap<>();
    vertices.forEach(v -> idMap.put(v.getId(), v.getPropertyValue("name").getString()));

    Set<String> tags = new HashSet<>(Arrays.asList("Databases", "Hadoop"));
    Set<String> persons = new HashSet<>(Arrays.asList("Eve", "Alice", "Frank", "Dave"));

    for(Edge e : newEdges) {
      String sourceName = idMap.get(e.getSourceId());
      String targetName = idMap.get(e.getTargetId());

      Assert.assertTrue("source: " + sourceName + " | target: " + targetName,
          tags.contains(sourceName) && persons.contains(targetName));
      persons.remove(targetName);
    }
  }
}
