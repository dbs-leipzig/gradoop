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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMEdgeFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperatorTest;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class FilterAndProjectEdgesTest extends PhysicalOperatorTest {

  @Test
  public void testFilterWithNoPredicates() throws Exception {
    CNF predicates = predicateFromQuery("MATCH ()-[a]->()");

    EPGMEdgeFactory edgeFactory = new EPGMEdgeFactory();
    Properties properties = Properties.create();
    properties.set("name", "Anton");
    EPGMEdge e1 = edgeFactory.createEdge("knows", GradoopId.get(), GradoopId.get(), properties);

    DataSet<EPGMEdge> edges = getExecutionEnvironment().fromElements(e1);

    List<Embedding> result = new FilterAndProjectEdges<>(edges, predicates, new ArrayList<>(), false)
      .evaluate()
      .collect();

    assertEquals(1, result.size());
    assertEquals(result.get(0).getId(1), e1.getId());
  }

  @Test
  public void testFilterEdgesByProperties() throws Exception {
    CNF predicates = predicateFromQuery("MATCH ()-[a]->() WHERE a.since > 2013");

    EPGMEdgeFactory edgeFactory = new EPGMEdgeFactory();
    Properties properties = Properties.create();
    properties.set("since", 2014);
    EPGMEdge e1 = edgeFactory.createEdge("knows", GradoopId.get(), GradoopId.get(), properties);

    properties = Properties.create();
    properties.set("since", 2013);
    EPGMEdge e2 = edgeFactory.createEdge("knows", GradoopId.get(), GradoopId.get(), properties);

    DataSet<EPGMEdge> edges = getExecutionEnvironment().fromElements(e1, e2);

    List<Embedding> result = new FilterAndProjectEdges<>(edges, predicates, new ArrayList<>(), false)
      .evaluate()
      .collect();

    assertEquals(1, result.size());
    assertEquals(result.get(0).getId(1), e1.getId());
  }

  @Test
  public void testFilterEdgesByLabel() throws Exception {
    CNF predicates = predicateFromQuery("MATCH ()-[a:likes]->()");

    EPGMEdgeFactory edgeFactory = new EPGMEdgeFactory();
    EPGMEdge e1 = edgeFactory.createEdge("likes", GradoopId.get(), GradoopId.get());
    EPGMEdge e2 = edgeFactory.createEdge("knows", GradoopId.get(), GradoopId.get());
    DataSet<EPGMEdge> edges = getExecutionEnvironment().fromElements(e1, e2);

    List<Embedding> result = new FilterAndProjectEdges<>(edges, predicates, new ArrayList<>(), false)
      .evaluate()
      .collect();

    assertEquals(1, result.size());
    assertEquals(result.get(0).getId(1), e1.getId());
  }

  @Test
  public void testResultingEntryList() throws Exception {
    CNF predicates = predicateFromQuery("MATCH ()-[a]->() WHERE a.name = \"Alice\"");

    Properties properties = Properties.create();
    properties.set("name", "Alice");
    EPGMEdge
      edge = new EPGMEdgeFactory().createEdge("Label", GradoopId.get(), GradoopId.get(), properties);

    DataSet<EPGMEdge> edges = getExecutionEnvironment().fromElements(edge);

    List<Embedding> result = new FilterAndProjectEdges<>(edges, predicates, new ArrayList<>(), false)
      .evaluate()
      .collect();

    assertEquals(edge.getSourceId(), result.get(0).getId(0));
    assertEquals(edge.getId(),       result.get(0).getId(1));
    assertEquals(edge.getTargetId(), result.get(0).getId(2));
  }

  @Test
  public void testProjectionOfAvailableValues() throws Exception {
    CNF predicates = predicateFromQuery("MATCH ()-[a]->() WHERE a.name = \"Alice\"");

    Properties properties = Properties.create();
    properties.set("name", "Alice");
    EPGMEdge
      edge = new EPGMEdgeFactory().createEdge("Label", GradoopId.get(), GradoopId.get(), properties);

    DataSet<EPGMEdge> edges = getExecutionEnvironment().fromElements(edge);

    List<String> projectionPropertyKeys = Lists.newArrayList("name");

    Embedding result = new FilterAndProjectEdges<>(edges, predicates, projectionPropertyKeys, false)
      .evaluate().collect().get(0);

    assertEquals(result.getProperty(0), PropertyValue.create("Alice"));
  }

  @Test
  public void testProjectionOfMissingValues() throws Exception {
    CNF predicates = predicateFromQuery("MATCH ()-[a]->() WHERE a.name = \"Alice\"");

    Properties properties = Properties.create();
    properties.set("name", "Alice");
    EPGMEdge
      edge = new EPGMEdgeFactory().createEdge("Label", GradoopId.get(), GradoopId.get(), properties);

    DataSet<EPGMEdge> edges = getExecutionEnvironment().fromElements(edge);

    List<String> projectionPropertyKeys = Lists.newArrayList("name", "since");

    Embedding result = new FilterAndProjectEdges<>(edges, predicates, projectionPropertyKeys, false)
      .evaluate().collect().get(0);

    assertEquals(result.getProperty(0), PropertyValue.create("Alice"));
    assertEquals(result.getProperty(1), PropertyValue.NULL_VALUE);
  }

  @Test
  public void testProjectLoop() throws Exception {
    CNF predicates = predicateFromQuery("MATCH (a)-[b]->(a)");

    GradoopId a = GradoopId.get();
    EPGMEdge edge = new EPGMEdgeFactory().createEdge(a, a);

    DataSet<EPGMEdge> edges = getExecutionEnvironment().fromElements(edge);

    Embedding result = new FilterAndProjectEdges<>(edges, predicates, Collections.emptyList(), true)
      .evaluate().collect().get(0);

    assertEquals(result.size(), 2);
    assertEquals(a, result.getId(0));
    assertEquals(edge.getId(), result.getId(1));
  }
}
