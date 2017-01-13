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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.common;

import com.google.common.collect.Lists;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingFactory;
import org.junit.Test;

import static org.junit.Assert.*;

public class EmbeddingFactoryTest {

  @Test
  public void convertVertex() throws Exception {
    Properties properties = new Properties();
    properties.set("foo", 1);
    properties.set("bar", "42");
    properties.set("baz", false);
    Vertex vertex = new VertexFactory().createVertex("TestVertex",  properties);

    Embedding embedding =
      EmbeddingFactory.fromVertex(vertex, Lists.newArrayList("foo","bar"));

    assertEquals(1, embedding.size());
    assertEquals(vertex.getId(), embedding.getId(0));
    assertEquals(PropertyValue.create(1), embedding.getProperty(0));
    assertEquals(PropertyValue.create("42"), embedding.getProperty(1));
  }

  @Test
  public void convertEdge() throws Exception {
    Properties properties = new Properties();
    properties.set("foo", 1);
    properties.set("bar", "42");
    properties.set("baz", false);
    Edge edge = new EdgeFactory().createEdge(
      "TestVertex", GradoopId.get(), GradoopId.get(), properties
    );

    Embedding embedding =
      EmbeddingFactory.fromEdge(edge, Lists.newArrayList("foo","bar"));

    assertEquals(3, embedding.size());
    assertEquals(edge.getSourceId(), embedding.getId(0));
    assertEquals(edge.getId(), embedding.getId(1));
    assertEquals(edge.getTargetId(), embedding.getId(2));
    assertEquals(PropertyValue.create(1), embedding.getProperty(0));
    assertEquals(PropertyValue.create("42"), embedding.getProperty(1));
  }
}