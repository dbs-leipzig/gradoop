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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.physical;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.ProjectionEntry;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class FilterAndProjectVerticesTest extends PhysicalOperatorTest {
  @Test
  public void testFilterVertices() throws Exception{
    CNF predicates = predicateFromQuery("MATCH (a) WHERE a.name = \"Alice\"");

    PropertyList properties = PropertyList.create();
    properties.set("name", "Anton");
    DataSet<Vertex> vertex = getExecutionEnvironment().fromCollection(
      Lists.newArrayList(
        new VertexFactory().createVertex("Person", properties)
      )
    );

    FilterAndProjectVertices operator = new FilterAndProjectVertices(vertex, predicates,
      Lists.newArrayList("name"));

    assertEquals(0, operator.evaluate().count());
  }

  @Test
  public void testKeepVertices() throws Exception{
    CNF predicates = predicateFromQuery("MATCH (a) WHERE a.name = \"Alice\"");

    PropertyList properties = PropertyList.create();
    properties.set("name", "Alice");
    DataSet<Vertex> vertex = getExecutionEnvironment().fromCollection(
      Lists.newArrayList(
        new VertexFactory().createVertex("Person", properties)
      )
    );

    FilterAndProjectVertices operator = new FilterAndProjectVertices(vertex, predicates,
      Lists.newArrayList("name"));

    assertEquals(1, operator.evaluate().count());
  }

  @Test
  public void testProjectRemainingVertices() throws Exception{
    CNF predicates = predicateFromQuery("MATCH (a) WHERE a.name = \"Alice\"");

    PropertyList propertiesMatch = PropertyList.create();
    propertiesMatch.set("name", "Alice");
    propertiesMatch.set("foo", "bar");

    PropertyList propertiesMiss = PropertyList.create();
    propertiesMiss.set("name", "Anton");
    propertiesMiss.set("foo", "baz");

    DataSet<Vertex> vertex = getExecutionEnvironment().fromCollection(
      Lists.newArrayList(
        new VertexFactory().createVertex("Person", propertiesMatch),
        new VertexFactory().createVertex("Person", propertiesMiss)
      )
    );

    FilterAndProjectVertices operator = new FilterAndProjectVertices(vertex, predicates,
      Lists.newArrayList("foo"));

    List<Embedding> result = operator.evaluate().collect();

    assertEquals(ProjectionEntry.class, result.get(0).getEntry(0).getClass());
    assertEquals(vertex.collect().get(0).getId(), result.get(0).getEntry(0).getId());
    assertEquals(
      Lists.newArrayList("foo"),
      result.get(0).getEntry(0).getProperties().get().getKeys()
    );
  }

}
