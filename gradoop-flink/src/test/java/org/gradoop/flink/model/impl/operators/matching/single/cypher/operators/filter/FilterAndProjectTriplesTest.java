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
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperatorTest;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Triple;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FilterAndProjectTriplesTest extends PhysicalOperatorTest {
  private VertexFactory vertexFactory;
  private EdgeFactory edgeFactory;

  @Before
  public void setUp() throws Exception {
    this.vertexFactory = new VertexFactory();
    this.edgeFactory = new EdgeFactory();
  }

  @Test
  public void testFilterWithNoPredicates() throws Exception {
    CNF predicates = predicateFromQuery("MATCH (a)-[e]->(b)");

    Properties properties = Properties.create();
    properties.set("name", "Alice");
    Vertex a = vertexFactory.createVertex("Person", properties);

    properties = Properties.create();
    properties.set("name", "Bob");
    Vertex b = vertexFactory.createVertex("Person", properties);

    properties = Properties.create();
    properties.set("since", "2013");
    Edge e = edgeFactory.createEdge("knows", a.getId(), b.getId(), properties);

    DataSet<Triple> triples = getExecutionEnvironment().fromElements(new Triple(a,e,b));

    List<Embedding> result = new FilterAndProjectTriples(
      triples,
      "a", "e", "b",
      predicates,
      new HashMap<>(), MatchStrategy.ISOMORPHISM
    ).evaluate().collect();
    
    assertEquals(1, result.size());
    assertTrue(result.get(0).getId(0).equals(a.getId()));
    assertTrue(result.get(0).getId(1).equals(e.getId()));
    assertTrue(result.get(0).getId(2).equals(b.getId()));
  }

  @Test
  public void testFilterByProperties() throws Exception {
    CNF predicates = predicateFromQuery("MATCH (a)-[e]->(b) WHERE a.age >= b.age " +
      "AND e.since = 2013");

    Properties properties = Properties.create();
    properties.set("name", "Alice");
    properties.set("age", "25");
    Vertex a = vertexFactory.createVertex("Person", properties);

    properties = Properties.create();
    properties.set("name", "Bob");
    properties.set("age", "24");
    Vertex b = vertexFactory.createVertex("Person", properties);

    properties = Properties.create();
    properties.set("since", 2013);
    Edge e1 = edgeFactory.createEdge("knows", a.getId(), b.getId(), properties);

    properties = Properties.create();
    properties.set("since", 2013);
    Edge e2 = edgeFactory.createEdge("knows", b.getId(), a.getId(), properties);

    properties = Properties.create();
    properties.set("since", 2014);
    Edge e3 = edgeFactory.createEdge("knows", a.getId(), b.getId(), properties);


    DataSet<Triple> triples = getExecutionEnvironment().fromElements(
      new Triple(a,e1,b),
      new Triple(b,e2,a),
      new Triple(a,e3,b)
    );

    List<Embedding> result = new FilterAndProjectTriples(triples,
      "a", "e", "b",
      predicates, new HashMap<>(), MatchStrategy.ISOMORPHISM
    ).evaluate().collect();

    assertEquals(1, result.size());
    assertTrue(result.get(0).getId(0).equals(a.getId()));
    assertTrue(result.get(0).getId(1).equals(e1.getId()));
    assertTrue(result.get(0).getId(2).equals(b.getId()));
  }

  @Test
  public void testFilterByLabel() throws Exception {
    CNF predicates = predicateFromQuery("MATCH (a:Person)-[e:likes]->(b)");

    Vertex a1 = vertexFactory.createVertex("Person");
    Vertex a2 = vertexFactory.createVertex("Orc");
    Vertex b = vertexFactory.createVertex("Person");
    Edge e1 = edgeFactory.createEdge("likes", a1.getId(), b.getId());
    Edge e2 = edgeFactory.createEdge("likes", a2.getId(), b.getId());
    Edge e3 = edgeFactory.createEdge("knows", a1.getId(), b.getId());

    DataSet<Triple> triples = getExecutionEnvironment().fromElements(
      new Triple(a1,e1,b),
      new Triple(a2,e2,b),
      new Triple(a1,e3,b)
    );


    List<Embedding> result = new FilterAndProjectTriples(triples,
      "a", "e", "b",
      predicates, new HashMap<>(), MatchStrategy.ISOMORPHISM
    ).evaluate().collect();

    assertEquals(1, result.size());
    assertTrue(result.get(0).getId(0).equals(a1.getId()));
    assertTrue(result.get(0).getId(1).equals(e1.getId()));
    assertTrue(result.get(0).getId(2).equals(b.getId()));
  }

  @Test
  public void testFilterBySelfLoop() throws Exception {
    CNF predicates = predicateFromQuery("MATCH (a)-[e]->(a)");

    Vertex a = vertexFactory.createVertex("Person");
    Vertex b = vertexFactory.createVertex("Person");
    Edge e1 = edgeFactory.createEdge("loves", a.getId(), a.getId());
    Edge e2 = edgeFactory.createEdge("loves", a.getId(), b.getId());

    DataSet<Triple> triples = getExecutionEnvironment().fromElements(
      new Triple(a,e1,a),
      new Triple(a,e2,b)
    );

    List<Embedding> result = new FilterAndProjectTriples(triples,
      "a", "e", "a",
      predicates, new HashMap<>(), MatchStrategy.ISOMORPHISM
    ).evaluate().collect();

    assertEquals(1, result.size());
    assertTrue(result.get(0).getId(0).equals(a.getId()));
    assertTrue(result.get(0).getId(1).equals(e1.getId()));
  }

  @Test
  public void testFilterByIsomorphism() throws Exception {
    CNF predicates = predicateFromQuery("MATCH (a)-[e]->(b)");

    Vertex a = vertexFactory.createVertex("Person");
    Vertex b = vertexFactory.createVertex("Person");
    Edge e1 = edgeFactory.createEdge("loves", a.getId(), a.getId());
    Edge e2 = edgeFactory.createEdge("loves", a.getId(), b.getId());

    DataSet<Triple> triples = getExecutionEnvironment().fromElements(
      new Triple(a,e1,a),
      new Triple(a,e2,b)
    );

    List<Embedding> result = new FilterAndProjectTriples(triples,
      "a", "e", "b",
      predicates, new HashMap<>(), MatchStrategy.ISOMORPHISM
      ).evaluate().collect();

    assertEquals(1, result.size());
    assertTrue(result.get(0).getId(0).equals(a.getId()));
    assertTrue(result.get(0).getId(1).equals(e2.getId()));
    assertTrue(result.get(0).getId(2).equals(b.getId()));
  }

  @Test
  public void testFilterByHomomorphism() throws Exception {
    CNF predicates = predicateFromQuery("MATCH (a)-[e]->(b)");

    Vertex a = vertexFactory.createVertex("Person");
    Vertex b = vertexFactory.createVertex("Person");
    Edge e1 = edgeFactory.createEdge("loves", a.getId(), a.getId());
    Edge e2 = edgeFactory.createEdge("loves", a.getId(), b.getId());

    DataSet<Triple> triples = getExecutionEnvironment().fromElements(
      new Triple(a,e1,a),
      new Triple(a,e2,b)
    );

    List<Embedding> result = new FilterAndProjectTriples(triples,
      "a", "e", "b",
      predicates, new HashMap<>(), MatchStrategy.HOMOMORPHISM
    ).evaluate().collect();

    assertEquals(2, result.size());
  }

  @Test
  public void testResultingEntryList() throws Exception {
    CNF predicates = predicateFromQuery("MATCH (a)-[e]->(b)");

    Vertex a = vertexFactory.createVertex("Person");
    Vertex b = vertexFactory.createVertex("Person");
    Edge e = edgeFactory.createEdge("loves", a.getId(), b.getId());

    DataSet<Triple> triples = getExecutionEnvironment().fromElements(new Triple(a,e,b));

    List<Embedding> result = new FilterAndProjectTriples(triples,
      "a", "e", "b",
      predicates, new HashMap<>(), MatchStrategy.ISOMORPHISM
    ).evaluate().collect();

    assertEquals(1, result.size());
    assertEquals(3, result.get(0).size());
    assertEquals(a.getId(), result.get(0).getId(0));
    assertEquals(e.getId(), result.get(0).getId(1));
    assertEquals(b.getId(), result.get(0).getId(2));
  }

  @Test
  public void testResultingEntryForSelfLoops() throws Exception {
    CNF predicates = predicateFromQuery("MATCH (a)-[e]->(a)");

    Vertex a = vertexFactory.createVertex("Person");
    Edge e = edgeFactory.createEdge("loves", a.getId(), a.getId());

    DataSet<Triple> triples = getExecutionEnvironment().fromElements(new Triple(a,e,a));

    List<Embedding> result = new FilterAndProjectTriples(triples,
      "a", "e", "a",
      predicates, new HashMap<>(), MatchStrategy.ISOMORPHISM
    ).evaluate().collect();

    assertEquals(1, result.size());
    assertEquals(2, result.get(0).size());
    assertEquals(a.getId(), result.get(0).getId(0));
    assertEquals(e.getId(), result.get(0).getId(1));
  }

  @Test
  public void testProjectionOfAvailableValues() throws Exception{
    CNF predicates = predicateFromQuery("MATCH (a)-[e]->(b)");

    Properties properties = Properties.create();
    properties.set("name", "Alice");
    properties.set("age", 25);
    Vertex a = vertexFactory.createVertex("Person", properties);

    properties = Properties.create();
    properties.set("name", "Bob");
    properties.set("age", 24);
    Vertex b = vertexFactory.createVertex("Person", properties);

    properties = Properties.create();
    properties.set("since", 2013);
    properties.set("active", true);
    Edge e = edgeFactory.createEdge("knows", a.getId(), b.getId(), properties);


    DataSet<Triple> triples = getExecutionEnvironment().fromElements(new Triple(a,e,b));

    HashMap<String, List<String>> propertyKeys = new HashMap<>();
    propertyKeys.put("a", Lists.newArrayList("name","age"));
    propertyKeys.put("e", Lists.newArrayList("since"));
    propertyKeys.put("b", Lists.newArrayList("name"));

    Embedding result = new FilterAndProjectTriples(triples,
      "a", "e", "b",
      predicates, propertyKeys, MatchStrategy.ISOMORPHISM
    ).evaluate().collect().get(0);

    assertTrue(result.getProperty(0).equals(PropertyValue.create("Alice")));
    assertTrue(result.getProperty(1).equals(PropertyValue.create(25)));
    assertTrue(result.getProperty(2).equals(PropertyValue.create(2013)));
    assertTrue(result.getProperty(3).equals(PropertyValue.create("Bob")));
  }

  @Test
  public void testProjectionOfMissingValues() throws Exception{
    CNF predicates = predicateFromQuery("MATCH (a)-[e]->(b)");

    Vertex a = vertexFactory.createVertex("Person");
    Vertex b = vertexFactory.createVertex("Person");
    Edge e = edgeFactory.createEdge("knows", a.getId(), b.getId());


    DataSet<Triple> triples = getExecutionEnvironment().fromElements(new Triple(a,e,b));

    HashMap<String, List<String>> propertyKeys = new HashMap<>();
    propertyKeys.put("a", Lists.newArrayList("name"));
    propertyKeys.put("e", Lists.newArrayList("since"));
    propertyKeys.put("b", Lists.newArrayList("name"));

    Embedding result = new FilterAndProjectTriples(triples,
      "a", "e", "b",
      predicates, propertyKeys, MatchStrategy.ISOMORPHISM
    ).evaluate().collect().get(0);

    assertTrue(result.getProperty(0).equals(PropertyValue.NULL_VALUE));
    assertTrue(result.getProperty(1).equals(PropertyValue.NULL_VALUE));
    assertTrue(result.getProperty(2).equals(PropertyValue.NULL_VALUE));
  }
}
