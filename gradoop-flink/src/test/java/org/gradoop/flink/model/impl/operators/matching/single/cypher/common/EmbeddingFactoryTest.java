/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.common;

import com.google.common.collect.Lists;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingFactory;
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
      EmbeddingFactory.fromEdge(edge, Lists.newArrayList("foo","bar"), false);

    assertEquals(3, embedding.size());
    assertEquals(edge.getSourceId(), embedding.getId(0));
    assertEquals(edge.getId(), embedding.getId(1));
    assertEquals(edge.getTargetId(), embedding.getId(2));
    assertEquals(PropertyValue.create(1), embedding.getProperty(0));
    assertEquals(PropertyValue.create("42"), embedding.getProperty(1));
  }

  @Test
  public void convertEdgeWithLoop() throws Exception {
    Properties properties = new Properties();
    properties.set("foo", 1);
    properties.set("bar", "42");
    properties.set("baz", false);

    GradoopId a = GradoopId.get();

    Edge edge = new EdgeFactory().createEdge(
      "TestVertex", a, a, properties
    );

    Embedding embedding =
      EmbeddingFactory.fromEdge(edge, Lists.newArrayList("foo","bar"), true);

    assertEquals(2, embedding.size());
    assertEquals(edge.getSourceId(), embedding.getId(0));
    assertEquals(edge.getId(), embedding.getId(1));
    assertEquals(PropertyValue.create(1), embedding.getProperty(0));
    assertEquals(PropertyValue.create("42"), embedding.getProperty(1));
  }
}