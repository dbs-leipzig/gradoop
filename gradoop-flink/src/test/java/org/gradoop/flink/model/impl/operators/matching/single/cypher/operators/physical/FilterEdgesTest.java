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
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.IdEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter.FilterEdges;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class FilterEdgesTest extends PhysicalOperatorTest {

  @Test
  public void testFilterEdges() throws Exception{
    CNF predicates = predicateFromQuery("MATCH ()-[a]->() WHERE a.name = \"Alice\"");

    Properties properties = Properties.create();
    properties.set("name", "Anton");
    DataSet<Edge> edge = getExecutionEnvironment().fromCollection(
      Lists.newArrayList(
        new EdgeFactory().createEdge("Label", GradoopId.get(), GradoopId.get(), properties)
      )
    );

    FilterEdges filter = new FilterEdges(edge, predicates);

    assertEquals(0, filter.evaluate().count());
  }

  @Test
  public void testFilterEdgesByLabel() throws Exception{
    CNF predicates = predicateFromQuery("MATCH ()-[a:likes]->()");

    DataSet<Edge> edge = getExecutionEnvironment().fromCollection(
      Lists.newArrayList(
        new EdgeFactory().createEdge("hates", GradoopId.get(), GradoopId.get(), Properties.create())
      )
    );

    FilterEdges filter = new FilterEdges(edge, predicates);

    assertEquals(0, filter.evaluate().count());
  }

  @Test
  public void testKeepEdges() throws Exception{
    CNF predicates = predicateFromQuery("MATCH ()-[a]->() WHERE a.name = \"Alice\"");

    Properties properties = Properties.create();
    properties.set("name", "Alice");
    DataSet<Edge> edge = getExecutionEnvironment().fromCollection(
      Lists.newArrayList(
        new EdgeFactory().createEdge("Label", GradoopId.get(), GradoopId.get(), properties)
      )
    );

    FilterEdges filter = new FilterEdges(edge, predicates);

    assertEquals(1, filter.evaluate().count());
  }

  @Test
  public void testKeepEdgesByLabel() throws Exception{
    CNF predicates = predicateFromQuery("MATCH ()-[a:likes]->()");

    DataSet<Edge> edge = getExecutionEnvironment().fromCollection(
      Lists.newArrayList(
        new EdgeFactory().createEdge("likes", GradoopId.get(), GradoopId.get(), Properties.create())
      )
    );

    FilterEdges filter = new FilterEdges(edge, predicates);

    assertEquals(1, filter.evaluate().count());
  }

  @Test
  public void testReturnEmbeddingWithThreeIdEntries() throws Exception{
    CNF predicates = predicateFromQuery("MATCH ()-[a]->() WHERE a.name = \"Alice\"");

    Properties properties = Properties.create();
    properties.set("name", "Alice");

    Edge edge = new EdgeFactory().createEdge("Label", GradoopId.get(), GradoopId.get(), properties);

    DataSet<Edge> edgeDataSet = getExecutionEnvironment().fromCollection(
      Lists.newArrayList(edge)
    );

    FilterEdges filter = new FilterEdges(edgeDataSet, predicates);

    List<Embedding> result = filter.evaluate().collect();

    assertEquals(IdEntry.class, result.get(0).getEntry(0).getClass());
    assertEquals(IdEntry.class, result.get(0).getEntry(1).getClass());
    assertEquals(IdEntry.class, result.get(0).getEntry(2).getClass());

    assertEquals(edge.getSourceId(), result.get(0).getEntry(0).getId());
    assertEquals(edge.getId(),       result.get(0).getEntry(1).getId());
    assertEquals(edge.getTargetId(), result.get(0).getEntry(2).getId());
  }
}
