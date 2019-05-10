/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.io.impl.dot.functions;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DotFileFormatSimpleTest extends GradoopFlinkTestBase {

  private GraphTransaction transaction;

  @Before
  public void initGraphTransactionMock() {
    GraphHead graphHead = new GraphHead();
    graphHead.setId(GradoopId.fromString("aaaaaaaaaaaaaaaaaaaaaaaa"));
    graphHead.setLabel("graph");

    Set<Vertex> vertices = new HashSet<>();
    Vertex vertex1 =  new Vertex();
    vertex1.setLabel("Person");
    vertex1.setProperty("name", "Tom");
    vertex1.setProperty("age", 25);
    GradoopId idVertex1 = GradoopId.fromString("bbbbbbbbbbbbbbbbbbbbbbbb");
    vertex1.setId(idVertex1);
    Vertex vertex2 = new Vertex();
    vertex2.setLabel("Forum");
    vertex2.setProperty("lan", "EN");
    GradoopId idVertex2 = GradoopId.fromString("cccccccccccccccccccccccc");
    vertex2.setId(idVertex2);
    Vertex vertex3 = new Vertex();
    vertex3.setLabel("Person");
    vertex3.setProperty("name", "Anna");
    vertex3.setProperty("age", 27);
    GradoopId idVertex3 = GradoopId.fromString("dddddddddddddddddddddddd");
    vertex3.setId(idVertex3);
    vertices.add(vertex1);
    vertices.add(vertex2);
    vertices.add(vertex3);

    Set<Edge> edges = new HashSet<>();
    Edge edge1 = new Edge();
    edge1.setSourceId(idVertex1);
    edge1.setTargetId(idVertex3);
    edge1.setLabel("knows");
    edge1.setId(GradoopId.fromString("eeeeeeeeeeeeeeeeeeeeeeee"));
    Edge edge2 = new Edge();
    edge2.setSourceId(idVertex3);
    edge2.setTargetId(idVertex1);
    edge2.setLabel("knows");
    edge2.setId(GradoopId.fromString("ffffffffffffffffffffffff"));
    Edge edge3 = new Edge();
    edge3.setSourceId(idVertex2);
    edge3.setTargetId(idVertex3);
    edge3.setLabel("hasModerator");
    edge3.setId(GradoopId.fromString("111111111111111111111111"));
    edges.add(edge1);
    edges.add(edge2);
    edges.add(edge3);

    GraphTransaction transactionMock = mock(GraphTransaction.class);
    when(transactionMock.getGraphHead()).thenReturn(graphHead);
    when(transactionMock.getVertices()).thenReturn(vertices);
    when(transactionMock.getEdges()).thenReturn(edges);

    this.transaction = transactionMock;
  }

  @Test
  public void testFormat() {
    DotFileFormatSimple dotFileFormat = new DotFileFormatSimple(true);

    String expected = "subgraph cluster_gaaaaaaaaaaaaaaaaaaaaaaaa{\n" +
      "label=\"graph\";\n" +
      "vddddddddddddddddddddddddaaaaaaaaaaaaaaaaaaaaaaaa [ label=\"Person\",name=\"Anna\",age=\"27\"];\n" +
      "vbbbbbbbbbbbbbbbbbbbbbbbbaaaaaaaaaaaaaaaaaaaaaaaa [ label=\"Person\",name=\"Tom\",age=\"25\"];\n" +
      "vccccccccccccccccccccccccaaaaaaaaaaaaaaaaaaaaaaaa [ label=\"Forum\",lan=\"EN\"];\n" +
      "vddddddddddddddddddddddddaaaaaaaaaaaaaaaaaaaaaaaa->vbbbbbbbbbbbbbbbbbbbbbbbbaaaaaaaaaaaaaaaaaaaaaaaa [label=\"knows\"];\n" +
      "vccccccccccccccccccccccccaaaaaaaaaaaaaaaaaaaaaaaa->vddddddddddddddddddddddddaaaaaaaaaaaaaaaaaaaaaaaa [label=\"hasModerator\"];\n" +
      "vbbbbbbbbbbbbbbbbbbbbbbbbaaaaaaaaaaaaaaaaaaaaaaaa->vddddddddddddddddddddddddaaaaaaaaaaaaaaaaaaaaaaaa [label=\"knows\"];\n" +
      "}\n";

    assertEquals(expected, dotFileFormat.format(transaction));
  }
}
