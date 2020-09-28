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

package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.filter;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperatorTest;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class FilterAndProjectTemporalVerticesTest extends PhysicalTPGMOperatorTest {

  @Test
  public void testFilterWithNoPredicates() throws Exception {
    CNF predicates = predicateFromQuery("MATCH (a)");

    Properties properties = Properties.create();
    properties.set("name", "Anton");
    TemporalVertex v = new TemporalVertexFactory().createVertex("Person", properties);
    v.setTransactionTime(new Tuple2<>(123L, 1234L));
    v.setValidTime(new Tuple2<>(678L, 6789L));
    DataSet<TemporalVertex> vertices = getExecutionEnvironment().fromElements(v);

    FilterAndProjectTemporalVertices filter =
      new FilterAndProjectTemporalVertices(vertices, predicates, new ArrayList<>());

    assertEquals(1, filter.evaluate().count());
  }

  @Test
  public void testFilterVerticesByProperties() throws Exception {
    CNF predicates = predicateFromQuery("MATCH (a) WHERE a.name = \"Alice\"");

    TemporalVertexFactory vertexFactory = new TemporalVertexFactory();
    Properties properties = Properties.create();
    properties.set("name", "Alice");
    TemporalVertex v1 = vertexFactory.createVertex("Person", properties);
    v1.setTransactionTime(new Tuple2<>(123L, 1234L));
    v1.setValidTime(new Tuple2<>(678L, 6789L));
    properties = Properties.create();
    properties.set("name", "Bob");
    TemporalVertex v2 = vertexFactory.createVertex("Person", properties);
    v2.setTransactionTime(new Tuple2<>(123L, 1234L));
    v2.setValidTime(new Tuple2<>(678L, 6789L));

    DataSet<TemporalVertex> vertices = getExecutionEnvironment().fromElements(v1, v2);


    List<Embedding> result =
      new FilterAndProjectTemporalVertices(vertices, predicates, new ArrayList<>())
        .evaluate()
        .collect();

    assertEquals(1, result.size());
    assertEquals(result.get(0).getId(0), v1.getId());
  }

  @Test
  public void testFilterVerticesByLabel() throws Exception {
    CNF predicates = predicateFromQuery("MATCH (a:Person)");

    TemporalVertexFactory vertexFactory = new TemporalVertexFactory();
    TemporalVertex v1 = vertexFactory.createVertex("Person");
    v1.setTransactionTime(new Tuple2<>(123L, 1234L));
    v1.setValidTime(new Tuple2<>(678L, 6789L));
    TemporalVertex v2 = vertexFactory.createVertex("Forum");
    v2.setTransactionTime(new Tuple2<>(123L, 1234L));
    v2.setValidTime(new Tuple2<>(678L, 6789L));
    DataSet<TemporalVertex> vertices = getExecutionEnvironment().fromElements(v1, v2);

    List<Embedding> result =
      new FilterAndProjectTemporalVertices(vertices, predicates, new ArrayList<>())
        .evaluate()
        .collect();

    assertEquals(1, result.size());
    assertEquals(result.get(0).getId(0), v1.getId());
  }

  @Test
  public void testFilterVerticesByTime() throws Exception {
    CNF predicates = predicateFromQuery(
      "MATCH (a:Person) WHERE a.tx_to.after(Timestamp(1970-01-01T00:01:00))").getSubCNF("a");

    TemporalVertexFactory vertexFactory = new TemporalVertexFactory();
    // v1 fulfills the predicate
    TemporalVertex v1 = vertexFactory.createVertex("Person");
    v1.setTransactionTime(new Tuple2<>(0L, 10000000L));
    v1.setValidTime(new Tuple2<>(678L, 6789L));
    // v2 does not fulfill the predicate
    TemporalVertex v2 = vertexFactory.createVertex("Forum");
    v2.setTransactionTime(new Tuple2<>(123L, 1234L));
    v2.setValidTime(new Tuple2<>(678L, 6789L));
    DataSet<TemporalVertex> vertices = getExecutionEnvironment().fromElements(v1, v2);

    List<Embedding> result =
      new FilterAndProjectTemporalVertices(vertices, predicates, new ArrayList<>())
        .evaluate()
        .collect();

    assertEquals(1, result.size());
    assertEquals(result.get(0).getId(0), v1.getId());
  }

  @Test
  public void testProjectionOfAvailableValues() throws Exception {
    CNF predicates = predicateFromQuery("MATCH (a:Person) WHERE a.name = \"Alice\"");

    Properties properties = Properties.create();
    properties.set("name", "Alice");
    TemporalVertex person = new TemporalVertexFactory().createVertex("Person", properties);
    person.setTransactionTime(new Tuple2<>(123L, 1234L));
    person.setValidTime(new Tuple2<>(678L, 6789L));
    DataSet<TemporalVertex> vertices = getExecutionEnvironment().fromElements(person);

    List<String> projectionPropertyKeys = Lists.newArrayList("name");

    Embedding result =
      new FilterAndProjectTemporalVertices(vertices, predicates, projectionPropertyKeys)
        .evaluate()
        .collect()
        .get(0);

    assertEquals(person.getId(), result.getId(0));
    assertEquals(result.getId(0), person.getId());
    assertEquals(result.getProperty(0), PropertyValue.create("Alice"));
  }

  @Test
  public void testProjectionOfMissingValues() throws Exception {
    CNF predicates = predicateFromQuery("MATCH (a) WHERE a.name = \"Alice\"");

    Properties properties = Properties.create();
    properties.set("name", "Alice");
    TemporalVertex person = new TemporalVertexFactory().createVertex("Person", properties);
    person.setTransactionTime(new Tuple2<>(123L, 1234L));
    person.setValidTime(new Tuple2<>(678L, 6789L));
    DataSource<TemporalVertex> vertices = getExecutionEnvironment().fromElements(person);

    List<String> projectionPropertyKeys = Lists.newArrayList("name", "age");

    Embedding result =
      new FilterAndProjectTemporalVertices(vertices, predicates, projectionPropertyKeys)
        .evaluate()
        .collect()
        .get(0);

    assertEquals(person.getId(), result.getId(0));
    assertEquals(result.getProperty(0), PropertyValue.create("Alice"));
    assertEquals(result.getProperty(1), PropertyValue.NULL_VALUE);
  }
}
