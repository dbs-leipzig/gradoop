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
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.IdEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.ProjectionEntry;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class FilterAndProjectEdgesTest extends PhysicalOperatorTest {
  @Test
  public void testFilterEdges() throws Exception{
    CNF predicates = predicateFromQuery("MATCH ()-[a]->() WHERE a.name = \"Alice\"");

    PropertyList properties = PropertyList.create();
    properties.set("name", "Anton");
    DataSet<Edge> edgeDataSet = getExecutionEnvironment().fromCollection(
      Lists.newArrayList(
        new EdgeFactory().createEdge("Label", GradoopId.get(), GradoopId.get(), properties)
      )
    );

    FilterAndProjectEdges operator = new FilterAndProjectEdges(edgeDataSet, predicates,
      Lists.newArrayList("name"));

    assertEquals(0, operator.evaluate().count());
  }

  @Test
  public void testKeepEdges() throws Exception{
    CNF predicates = predicateFromQuery("MATCH ()-[a]->() WHERE a.name = \"Alice\"");

    PropertyList properties = PropertyList.create();
    properties.set("name", "Alice");
    DataSet<Edge> edgeDataSet = getExecutionEnvironment().fromCollection(
      Lists.newArrayList(
        new EdgeFactory().createEdge("Label", GradoopId.get(), GradoopId.get(), properties)
      )
    );

    FilterAndProjectEdges operator = new FilterAndProjectEdges(edgeDataSet, predicates,
      Lists.newArrayList("name"));

    assertEquals(1, operator.evaluate().count());
  }

  @Test
  public void testProjectRemainingEdges() throws Exception{
    CNF predicates = predicateFromQuery("MATCH ()-[a]->() WHERE a.name = \"Alice\"");

    PropertyList propertiesMatch = PropertyList.create();
    propertiesMatch.set("name", "Alice");
    propertiesMatch.set("foo", "bar");
    Edge edgeMatch =
      new EdgeFactory().createEdge("Label", GradoopId.get(), GradoopId.get(), propertiesMatch);

    PropertyList propertiesMiss = PropertyList.create();
    propertiesMiss.set("name", "Anton");
    propertiesMiss.set("foo", "baz");
    Edge edgeMiss =
      new EdgeFactory().createEdge("Label", GradoopId.get(), GradoopId.get(), propertiesMiss);

    DataSet<Edge> edgeDataSet = getExecutionEnvironment().fromCollection(
      Lists.newArrayList(
        edgeMatch,
        edgeMiss
      )
    );

    FilterAndProjectEdges operator = new FilterAndProjectEdges(edgeDataSet, predicates,
      Lists.newArrayList("foo"));

    List<Embedding> result = operator.evaluate().collect();

    assertEquals(IdEntry.class, result.get(0).getEntry(0).getClass());
    assertEquals(ProjectionEntry.class, result.get(0).getEntry(1).getClass());
    assertEquals(IdEntry.class, result.get(0).getEntry(2).getClass());

    assertEquals(edgeMatch.getSourceId(), result.get(0).getEntry(0).getId());
    assertEquals(edgeMatch.getId(),       result.get(0).getEntry(1).getId());
    assertEquals(edgeMatch.getTargetId(), result.get(0).getEntry(2).getId());

    assertEquals(
      Lists.newArrayList("foo"),
      result.get(0).getEntry(1).getProperties().get().getKeys()
    );
  }
}
