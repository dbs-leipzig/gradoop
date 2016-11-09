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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.utils;

import com.google.common.collect.Lists;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.EmbeddingEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.IdEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.ProjectionEntry;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PropertyProjectorTest {

  @Test
  public void projectGraphElementWithExistingPropertyKeysTest() throws Exception{
    VertexFactory vertexFactory = new VertexFactory();
    Vertex vertex = vertexFactory
      .createVertex("Label", getPropertyList(Lists.newArrayList("foo", "bar", "baz")));

    ArrayList<String> extractedPropertyKeys = Lists.newArrayList("foo", "bar");
    PropertyProjector projector = new PropertyProjector(extractedPropertyKeys);

    ProjectionEntry result = projector.project(vertex);

    assertEquals(extractedPropertyKeys,result.getProperties().get().getKeys());
    assertEquals(PropertyValue.create("foo"),result.getProperties().get().get("foo"));
  }

  @Test
  public void projectGraphElementWithMissingKeys() throws Exception{
    VertexFactory vertexFactory = new VertexFactory();
    Vertex vertex = vertexFactory.createVertex("Label",getPropertyList(Lists.newArrayList("foo", "baz")));

    ArrayList<String> extractedPropertyKeys = Lists.newArrayList("foo", "bar");
    PropertyProjector projector = new PropertyProjector(extractedPropertyKeys);

    ProjectionEntry result = projector.project(vertex);

    assertEquals(extractedPropertyKeys,result.getProperties().get().getKeys());
    assertTrue(result.getProperties().get().get("bar").isNull());
  }

  @Test
  public void graphElementProjectionHasCorrectId() throws Exception{
    VertexFactory vertexFactory = new VertexFactory();
    Vertex vertex = vertexFactory.createVertex("Label", new PropertyList());

    ArrayList<String> extractedPropertyKeys = Lists.newArrayList();
    PropertyProjector projector = new PropertyProjector(extractedPropertyKeys);

    ProjectionEntry result = projector.project(vertex);

    assertEquals(vertex.getId(), result.getId());
  }

  @Test
  public void graphElementProjectionIncludesLabel() throws Exception{
    VertexFactory vertexFactory = new VertexFactory();
    Vertex vertex = vertexFactory.createVertex("Person", new PropertyList());

    ArrayList<String> extractedPropertyKeys = Lists.newArrayList("__label__");
    PropertyProjector projector = new PropertyProjector(extractedPropertyKeys);

    ProjectionEntry result = projector.project(vertex);

    assertEquals(PropertyValue.create("Person"), result.getProperties().get().get("__label__"));
  }

  @Test
  public void projectEmbeddingWithExistingPropertyKeysTest() throws Exception{
    ProjectionEntry entry = new ProjectionEntry(GradoopId.get(),
      getPropertyList(Lists.newArrayList("foo", "bar", "baz")));

    ArrayList<String> extractedPropertyKeys = Lists.newArrayList("foo", "bar");
    PropertyProjector projector = new PropertyProjector(extractedPropertyKeys);

    ProjectionEntry result = projector.project(entry);

    assertEquals(extractedPropertyKeys, result.getProperties().get().getKeys());
    assertEquals(PropertyValue.create("foo"), result.getProperties().get().get("foo"));
  }

  @Test
  public void embeddingEntryProjectionHasCorrectId() throws Exception{
    ProjectionEntry entry = new ProjectionEntry(GradoopId.get(),
      getPropertyList(Lists.newArrayList("foo", "bar", "baz")));

    ArrayList<String> extractedPropertyKeys = Lists.newArrayList();
    PropertyProjector projector = new PropertyProjector(extractedPropertyKeys);

    ProjectionEntry result = projector.project(entry);

    assertEquals(entry.getId(), result.getId());
  }

  @Test
  public void returnEmptyProjectionEntryIfEntryHasNoProperties() throws Exception{
    EmbeddingEntry entry = new IdEntry(GradoopId.get());

    ArrayList<String> extractedPropertyKeys = Lists.newArrayList();
    PropertyProjector projector = new PropertyProjector(extractedPropertyKeys);

    ProjectionEntry result = projector.project(entry);

    assertEquals(0, result.getProperties().get().size());
  }

  private PropertyList getPropertyList(List<String> property_names) {
    PropertyList properties = new PropertyList();

    for(String property_name : property_names) {
      properties.set(property_name, property_name);
    }

    return properties;
  }
}
