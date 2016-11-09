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
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.IdEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.ProjectionEntry;

import org.junit.Test;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ProjectEdgesTest extends GradoopFlinkTestBase {

  @Test
  public void returnsEmbeddingWithIdProjectionId() throws Exception{
    DataSet<Edge> edgeDataSet = edgesForProperties(getPropertyList(Lists.newArrayList("foo", "bar", "baz")));

    ArrayList<String> extractedPropertyKeys = Lists.newArrayList("foo", "bar");
    ProjectEdges operator = new ProjectEdges(edgeDataSet, extractedPropertyKeys);

    List<Embedding> results = operator.evaluate().collect();

    assertEquals(2,results.size());
    assertEquals(3,results.get(0).size());

    Embedding embedding = results.get(0);
    assertEquals(IdEntry.class,         embedding.getEntry(0).getClass());
    assertEquals(ProjectionEntry.class, embedding.getEntry(1).getClass());
    assertEquals(IdEntry.class,         embedding.getEntry(2).getClass());

    assertEquals(extractedPropertyKeys,embedding.getEntry(1).getProperties().get().getKeys());
  }

  @Test
  public void returnsIdEntryOnEmptyPropertyListTest() throws Exception{
    DataSet<Edge> edgeDataSet = edgesForProperties(getPropertyList(Lists.newArrayList("foo", "bar", "baz")));

    ArrayList<String> extractedPropertyKeys = Lists.newArrayList();
    ProjectEdges operator = new ProjectEdges(edgeDataSet, extractedPropertyKeys);

    List<Embedding> results = operator.evaluate().collect();

    assertEquals(2,results.size());
    assertEquals(3,results.get(0).size());

    Embedding embedding = results.get(0);
    assertEquals(IdEntry.class,         embedding.getEntry(0).getClass());
    assertEquals(IdEntry.class, embedding.getEntry(1).getClass());
    assertEquals(IdEntry.class,         embedding.getEntry(2).getClass());
  }

  private PropertyList getPropertyList(List<String> property_names) {
    PropertyList properties = new PropertyList();

    for(String property_name : property_names) {
      properties.set(property_name, property_name);
    }

    return properties;
  }

  private DataSet<Edge> edgesForProperties(PropertyList properties) {
    EdgeFactory edgeFactory = new EdgeFactory();

    List<Edge> edges = Lists.newArrayList(
      edgeFactory.createEdge("Label1", GradoopId.get(), GradoopId.get(), properties),
      edgeFactory.createEdge("Label2", GradoopId.get(), GradoopId.get(), properties)
    );

    return getExecutionEnvironment().fromCollection(edges);
  }
}
