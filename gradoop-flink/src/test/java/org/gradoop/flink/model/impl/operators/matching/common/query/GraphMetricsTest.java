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
package org.gradoop.flink.model.impl.operators.matching.common.query;

import com.google.common.collect.Sets;
import org.junit.Test;
import org.s1ck.gdl.model.Vertex;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandlerTest.ECC_PROPERTY_KEY;
import static org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandlerTest.QUERY_HANDLER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GraphMetricsTest {

  @Test
  public void testGetDiameter() throws Exception {
    assertEquals(2, GraphMetrics.getDiameter(QUERY_HANDLER));
  }

  @Test
  public void testGetRadius() throws Exception {
    assertEquals(1, GraphMetrics.getRadius(QUERY_HANDLER));
  }

  @Test
  public void testGetEccentricity() throws Exception {
    Map<Long, Integer> eccentricity = GraphMetrics
      .getEccentricity(QUERY_HANDLER);

    for (Map.Entry<Long, Integer> ecc : eccentricity.entrySet()) {
      Integer expected = (Integer) QUERY_HANDLER
        .getVertexById(ecc.getKey())
        .getProperties().get(ECC_PROPERTY_KEY);
      assertEquals(expected, ecc.getValue());
    }
  }

  @Test
  public void testGetComponentsForSingleComponent() {
    QueryHandler query = QUERY_HANDLER;
    Set<String> allVertexVariables = query
      .getVertices()
      .stream()
      .map(Vertex::getVariable)
      .collect(Collectors.toSet());

    Map<Integer, Set<String>> components = GraphMetrics.getComponents(query);
    assertEquals(1, components.size());
    assertTrue(components.containsKey(0));
    assertEquals(allVertexVariables, components.get(0));
  }

  @Test
  public void testGetComponentsForMultipleComponent() {
    String queryString = "" +
      "(v1)-[e1]->(v2)" +
      "(v2)<-[e2]-(v3)" +
      "(v2)-[e3]->(v1)" +
      "(v3)-[e4]->(v3)" +
      "               " +
      "(v4)-[e5]->(v5)" +
      "(v6)-[e6]->(v5)";

    QueryHandler query = new QueryHandler(queryString);

    Map<Integer, Set<String>> components = GraphMetrics.getComponents(query);
    assertEquals(2, components.size());
    assertTrue(components.containsKey(0));
    assertTrue(components.containsKey(1));
    assertEquals(Sets.newHashSet("v1","v2","v3"), components.get(0));
    assertEquals(Sets.newHashSet("v4","v5","v6"), components.get(1));
  }
}