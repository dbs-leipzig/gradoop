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

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperatorTest;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FilterAndProjectEdgesTest extends PhysicalOperatorTest {

  @Test
  public void testFilterEdges() throws Exception {
    CNF predicates = predicateFromQuery("MATCH ()-[a]->() WHERE a.name = \"Alice\"");

    Properties properties = Properties.create();
    properties.set("name", "Anton");
    DataSet<Edge> edges = getExecutionEnvironment().fromElements(
      new EdgeFactory().createEdge("Label", GradoopId.get(), GradoopId.get(), properties));

    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setEntryColumn("a", 1);
    metaData.setPropertyColumn("a", "name", 0);

    FilterAndProjectEdges filter = new FilterAndProjectEdges(edges, predicates, metaData);

    assertEquals(0, filter.evaluate().count());
  }

  @Test
  public void testFilterEdgesByLabel() throws Exception {
    CNF predicates = predicateFromQuery("MATCH ()-[a:likes]->()");

    DataSet<Edge> edges = getExecutionEnvironment().fromElements(
      new EdgeFactory().createEdge("hates", GradoopId.get(), GradoopId.get()));

    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setEntryColumn("a", 1);
    metaData.setPropertyColumn("a", "__label__", 0);

    FilterAndProjectEdges filter = new FilterAndProjectEdges(edges, predicates, metaData);

    assertEquals(0, filter.evaluate().count());
  }

  @Test
  public void testKeepEdges() throws Exception {
    CNF predicates = predicateFromQuery("MATCH ()-[a]->() WHERE a.name = \"Alice\"");

    Properties properties = Properties.create();
    properties.set("name", "Alice");
    DataSet<Edge> edges = getExecutionEnvironment().fromElements(
      new EdgeFactory().createEdge("Label", GradoopId.get(), GradoopId.get(), properties));

    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setEntryColumn("a", 1);
    metaData.setPropertyColumn("a", "name", 0);

    FilterAndProjectEdges filter = new FilterAndProjectEdges(edges, predicates, metaData);

    assertEquals(1, filter.evaluate().count());
  }

  @Test
  public void testKeepEdgesByLabel() throws Exception {
    CNF predicates = predicateFromQuery("MATCH ()-[a:likes]->()");

    DataSet<Edge> edges = getExecutionEnvironment().fromElements(
      new EdgeFactory().createEdge("likes", GradoopId.get(), GradoopId.get()));

    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setEntryColumn("a", 1);
    metaData.setPropertyColumn("a", "__label__", 0);

    FilterAndProjectEdges filter = new FilterAndProjectEdges(edges, predicates, metaData);

    assertEquals(1, filter.evaluate().count());
  }

  @Test
  public void testReturnEmbeddingWithThreeIdEntries() throws Exception {
    CNF predicates = predicateFromQuery("MATCH ()-[a]->() WHERE a.name = \"Alice\"");

    Properties properties = Properties.create();
    properties.set("name", "Alice");
    Edge edge = new EdgeFactory().createEdge("Label", GradoopId.get(), GradoopId.get(), properties);

    DataSet<Edge> edges = getExecutionEnvironment().fromElements(edge);

    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setEntryColumn("a", 1);
    metaData.setPropertyColumn("a", "name", 0);

    FilterAndProjectEdges filter = new FilterAndProjectEdges(edges, predicates, metaData);

    List<Embedding> result = filter.evaluate().collect();

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

    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setEntryColumn("a", 1);
    metaData.setPropertyColumn("a", "name", 0);

    Embedding result = new FilterAndProjectEdges(edges, predicates, metaData)
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

    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setEntryColumn("a", 1);
    metaData.setPropertyColumn("a", "__label__", 0);
    metaData.setPropertyColumn("a", "name", 1);
    metaData.setPropertyColumn("a", "since", 2);

    Embedding result = new FilterAndProjectEdges(edges, predicates, metaData)
      .evaluate().collect().get(0);

    assertTrue(result.getProperty(0).equals(PropertyValue.create("Label")));
    assertTrue(result.getProperty(1).equals(PropertyValue.create("Alice")));
    assertTrue(result.getProperty(2).equals(PropertyValue.NULL_VALUE));
  }
}
