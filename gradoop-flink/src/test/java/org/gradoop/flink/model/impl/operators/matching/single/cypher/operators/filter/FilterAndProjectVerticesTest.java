/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSource;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperatorTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FilterAndProjectVerticesTest extends PhysicalOperatorTest {

  @Test
  public void testFilterWithNoPredicates() throws Exception {
    CNF predicates = predicateFromQuery("MATCH (a)");

    Properties properties = Properties.create();
    properties.set("name", "Anton");
    DataSet<Vertex> vertices = getExecutionEnvironment()
      .fromElements(new VertexFactory().createVertex("Person", properties));

    FilterAndProjectVertices filter =
      new FilterAndProjectVertices(vertices, predicates, new ArrayList<>());

    assertEquals(1, filter.evaluate().count());
  }

  @Test
  public void testFilterVerticesByProperties() throws Exception {
    CNF predicates = predicateFromQuery("MATCH (a) WHERE a.name = \"Alice\"");

    VertexFactory vertexFactory = new VertexFactory();
    Properties properties = Properties.create();
    properties.set("name", "Alice");
    Vertex v1 = vertexFactory.createVertex("Person", properties);
    properties = Properties.create();
    properties.set("name", "Bob");
    Vertex v2 = vertexFactory.createVertex("Person", properties);

    DataSet<Vertex> vertices = getExecutionEnvironment().fromElements(v1, v2);


    List<Embedding> result =
      new FilterAndProjectVertices(vertices, predicates, new ArrayList<>())
        .evaluate()
        .collect();

    assertEquals(1, result.size());
    assertTrue(result.get(0).getId(0).equals(v1.getId()));
  }

  @Test
  public void testFilterVerticesByLabel() throws Exception {
    CNF predicates = predicateFromQuery("MATCH (a:Person)");


    VertexFactory vertexFactory = new VertexFactory();
    Vertex v1 = vertexFactory.createVertex("Person");
    Vertex v2 = vertexFactory.createVertex("Forum");
    DataSet<Vertex> vertices = getExecutionEnvironment().fromElements(v1, v2);

    List<Embedding> result =
      new FilterAndProjectVertices(vertices, predicates, new ArrayList<>())
      .evaluate()
      .collect();

    assertEquals(1, result.size());
    assertTrue(result.get(0).getId(0).equals(v1.getId()));
  }

  @Test
  public void testProjectionOfAvailableValues() throws Exception {
    CNF predicates = predicateFromQuery("MATCH (a:Person) WHERE a.name = \"Alice\"");

    Properties properties = Properties.create();
    properties.set("name", "Alice");
    Vertex person = new VertexFactory().createVertex("Person", properties);
    DataSet<Vertex> vertices = getExecutionEnvironment().fromElements(person);

    List<String> projectionPropertyKeys = Lists.newArrayList("name");

    Embedding result =
      new FilterAndProjectVertices(vertices, predicates, projectionPropertyKeys)
      .evaluate()
      .collect()
      .get(0);

    assertEquals(person.getId(), result.getId(0));
    assertTrue(result.getId(0).equals(person.getId()));
    assertTrue(result.getProperty(0).equals(PropertyValue.create("Alice")));
  }

  @Test
  public void testProjectionOfMissingValues() throws Exception {
    CNF predicates = predicateFromQuery("MATCH (a) WHERE a.name = \"Alice\"");

    Properties properties = Properties.create();
    properties.set("name", "Alice");
    Vertex person = new VertexFactory().createVertex("Person", properties);
    DataSource<Vertex> vertices = getExecutionEnvironment().fromElements(person);

    List<String> projectionPropertyKeys = Lists.newArrayList("name","age");

    Embedding result =
      new FilterAndProjectVertices(vertices, predicates, projectionPropertyKeys)
      .evaluate()
      .collect()
      .get(0);

    assertEquals(person.getId(), result.getId(0));
    assertTrue(result.getProperty(0).equals(PropertyValue.create("Alice")));
    assertTrue(result.getProperty(1).equals(PropertyValue.NULL_VALUE));
  }
}
