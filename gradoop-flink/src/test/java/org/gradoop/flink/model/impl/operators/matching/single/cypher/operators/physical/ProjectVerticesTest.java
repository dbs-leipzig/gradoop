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
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import java.util.ArrayList;
import java.util.List;

import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.IdEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.ProjectionEntry;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class ProjectVerticesTest extends GradoopFlinkTestBase {

  @Test
  public void returnsEmbeddingWithOneProjection() throws Exception{
    DataSet<Vertex> vertexDataSet = verticesForProperties( getPropertyList(Lists.newArrayList("foo", "bar", "baz")));

    ArrayList<String> extractedPropertyKeys = Lists.newArrayList("foo", "bar");
    ProjectVertices operator = new ProjectVertices(vertexDataSet, extractedPropertyKeys);

    List<Embedding> results = operator.evaluate().collect();

    assertEquals(2,results.size());
    assertEquals(1,results.get(0).size());
    assertEquals(ProjectionEntry.class, results.get(0).getEntry(0).getClass());
    assertEquals(extractedPropertyKeys,results.get(0).getEntry(0).getProperties().get().getKeys());
  }

  @Test
  public void returnsIdEntryOnEmtpyPropertyListTets() throws Exception{
    DataSet<Vertex> vertexDataSet = verticesForProperties( getPropertyList(Lists.newArrayList("foo", "bar", "baz")));

    ArrayList<String> extractedPropertyKeys = Lists.newArrayList();
    ProjectVertices operator = new ProjectVertices(vertexDataSet, extractedPropertyKeys);

    List<Embedding> results = operator.evaluate().collect();

    assertEquals(2,results.size());
    assertEquals(1,results.get(0).size());
    assertEquals(IdEntry.class, results.get(0).getEntry(0).getClass());
  }



  private PropertyList getPropertyList(List<String> property_names) {
    PropertyList properties = new PropertyList();

    for(String property_name : property_names) {
      properties.set(property_name, property_name);
    }

    return properties;
  }

  private DataSet<Vertex> verticesForProperties(PropertyList properties) {
    VertexFactory vertexFactory = new VertexFactory();

    List<Vertex> vertices = Lists.newArrayList(
      vertexFactory.createVertex("Label1",properties),
      vertexFactory.createVertex("Label2",properties)
    );

    return getExecutionEnvironment().fromCollection(vertices);
  }
}
