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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperatorTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FilterAndProjectEdgesTest extends PhysicalOperatorTest {

  @Test
  public void testFilterWithNoPredicates() throws Exception {
    CNF predicates = predicateFromQuery("MATCH ()-[a]->()");

    EdgeFactory edgeFactory = new EdgeFactory();
    Properties properties = Properties.create();
    properties.set("name", "Anton");
    Edge e1 = edgeFactory.createEdge("knows", GradoopId.get(), GradoopId.get(), properties);

    DataSet<Edge> edges = getExecutionEnvironment().fromElements(e1);

    List<Embedding> result = new FilterAndProjectEdges(edges, predicates, new ArrayList<>(), false)
      .evaluate()
      .collect();

    assertEquals(1, result.size());
    assertTrue(result.get(0).getId(1).equals(e1.getId()));
  }

  @Test
  public void testFilterEdgesByProperties() throws Exception {
    CNF predicates = predicateFromQuery("MATCH ()-[a]->() WHERE a.since > 2013");

    EdgeFactory edgeFactory = new EdgeFactory();
    Properties properties = Properties.create();
    properties.set("since", 2014);
    Edge e1 = edgeFactory.createEdge("knows", GradoopId.get(), GradoopId.get(), properties);

    properties = Properties.create();
    properties.set("since", 2013);
    Edge e2 = edgeFactory.createEdge("knows", GradoopId.get(), GradoopId.get(), properties);

    DataSet<Edge> edges = getExecutionEnvironment().fromElements(e1, e2);

    List<Embedding> result = new FilterAndProjectEdges(edges, predicates, new ArrayList<>(), false)
      .evaluate()
      .collect();

    assertEquals(1, result.size());
    assertTrue(result.get(0).getId(1).equals(e1.getId()));
  }

  @Test
  public void testFilterEdgesByLabel() throws Exception {
    CNF predicates = predicateFromQuery("MATCH ()-[a:likes]->()");

    EdgeFactory edgeFactory = new EdgeFactory();
    Edge e1 = edgeFactory.createEdge("likes", GradoopId.get(), GradoopId.get());
    Edge e2 = edgeFactory.createEdge("knows", GradoopId.get(), GradoopId.get());
    DataSet<Edge> edges = getExecutionEnvironment().fromElements(e1, e2);

    List<Embedding> result = new FilterAndProjectEdges(edges, predicates, new ArrayList<>(), false)
      .evaluate()
      .collect();

    assertEquals(1, result.size());
    assertTrue(result.get(0).getId(1).equals(e1.getId()));
  }

  @Test
  public void testResultingEntryList() throws Exception {
    CNF predicates = predicateFromQuery("MATCH ()-[a]->() WHERE a.name = \"Alice\"");

    Properties properties = Properties.create();
    properties.set("name", "Alice");
    Edge edge = new EdgeFactory().createEdge("Label", GradoopId.get(), GradoopId.get(), properties);

    DataSet<Edge> edges = getExecutionEnvironment().fromElements(edge);

    List<Embedding> result = new FilterAndProjectEdges(edges, predicates, new ArrayList<>(), false)
      .evaluate()
      .collect();

    assertEquals(edge.getSourceId(), result.get(0).getId(0));
    assertEquals(edge.getId(),       result.get(0).getId(1));
    assertEquals(edge.getTargetId(), result.get(0).getId(2));
  }

  @Test
  public void testProjectionOfAvailableValues() throws Exception{
    CNF predicates = predicateFromQuery("MATCH ()-[a]->() WHERE a.name = \"Alice\"");

    Properties properties = Properties.create();
    properties.set("name", "Alice");
    Edge edge = new EdgeFactory().createEdge("Label", GradoopId.get(), GradoopId.get(), properties);

    DataSet<Edge> edges = getExecutionEnvironment().fromElements(edge);

    List<String> projectionPropertyKeys = Lists.newArrayList("name");

    Embedding result = new FilterAndProjectEdges(edges, predicates, projectionPropertyKeys, false)
      .evaluate().collect().get(0);

    assertTrue(result.getProperty(0).equals(PropertyValue.create("Alice")));
  }

  @Test
  public void testProjectionOfMissingValues() throws Exception{
    CNF predicates = predicateFromQuery("MATCH ()-[a]->() WHERE a.name = \"Alice\"");

    Properties properties = Properties.create();
    properties.set("name", "Alice");
    Edge edge = new EdgeFactory().createEdge("Label", GradoopId.get(), GradoopId.get(), properties);

    DataSet<Edge> edges = getExecutionEnvironment().fromElements(edge);

    List<String> projectionPropertyKeys = Lists.newArrayList("name","since");

    Embedding result = new FilterAndProjectEdges(edges, predicates, projectionPropertyKeys, false)
      .evaluate().collect().get(0);

    assertTrue(result.getProperty(0).equals(PropertyValue.create("Alice")));
    assertTrue(result.getProperty(1).equals(PropertyValue.NULL_VALUE));
  }

  @Test
  public void testProjectLoop() throws Exception {
    CNF predicates = predicateFromQuery("MATCH (a)-[b]->(a)");

    GradoopId a = GradoopId.get();
    Edge edge = new EdgeFactory().createEdge(a, a);

    DataSet<Edge> edges = getExecutionEnvironment().fromElements(edge);

    Embedding result = new FilterAndProjectEdges(edges, predicates, Collections.emptyList(), true)
      .evaluate().collect().get(0);

    assertEquals(result.size(), 2);
    assertEquals(a, result.getId(0));
    assertEquals(edge.getId(), result.getId(1));
  }
}
