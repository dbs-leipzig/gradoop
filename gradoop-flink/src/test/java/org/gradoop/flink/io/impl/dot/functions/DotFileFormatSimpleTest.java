/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMGraphHeadFactory;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.pojo.EPGMEdgeFactory;
import org.gradoop.common.model.impl.pojo.EPGMVertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test class for validating that {@link DotFileFormatSimple} formats a transaction as expected.
 */
public class DotFileFormatSimpleTest extends GradoopFlinkTestBase {

  private GraphTransaction transaction;

  @Before
  public void initGraphTransactionMock() {
    EPGMGraphHead graphHead = new EPGMGraphHeadFactory()
      .initGraphHead(GradoopId.fromString("aaaaaaaaaaaaaaaaaaaaaaaa"), "graph");

    // init vertex 1
    Map<String, Object> propertiesMap1 = new HashMap<>();
    propertiesMap1.put("name", "Tom");
    propertiesMap1.put("age", 25);
    GradoopId idVertex1 = GradoopId.fromString("bbbbbbbbbbbbbbbbbbbbbbbb");
    EPGMVertex vertex1 = new EPGMVertexFactory().initVertex(
            idVertex1, "Person", Properties.createFromMap(propertiesMap1));
    // init vertex 2
    Map<String, Object> propertiesMap2 = new HashMap<>();
    propertiesMap2.put("lan", "EN");
    GradoopId idVertex2 = GradoopId.fromString("cccccccccccccccccccccccc");
    EPGMVertex vertex2 = new EPGMVertexFactory().initVertex(
            idVertex2, "Forum", Properties.createFromMap(propertiesMap2));
    // init vertex 3
    Map<String, Object> propertiesMap3 = new HashMap<>();
    propertiesMap3.put("name", "Anna");
    propertiesMap3.put("age", 27);
    GradoopId idVertex3 = GradoopId.fromString("dddddddddddddddddddddddd");
    EPGMVertex vertex3 = new EPGMVertexFactory().initVertex(
            idVertex3, "Person", Properties.createFromMap(propertiesMap3));
    // create vertex set
    Set<EPGMVertex> vertices = new HashSet<>();
    vertices.add(vertex1);
    vertices.add(vertex2);
    vertices.add(vertex3);
    // init edge 1
    GradoopId idEdge1 = GradoopId.fromString("eeeeeeeeeeeeeeeeeeeeeeee");
    EPGMEdge edge1 = new EPGMEdgeFactory().initEdge(idEdge1, "knows", idVertex1, idVertex3);
    // init edge 2
    GradoopId idEdge2 = GradoopId.fromString("ffffffffffffffffffffffff");
    EPGMEdge edge2 = new EPGMEdgeFactory().initEdge(idEdge2, "knows", idVertex3, idVertex1);
    // init edge 3
    GradoopId idEdge3 = GradoopId.fromString("111111111111111111111111");
    EPGMEdge edge3 = new EPGMEdgeFactory().initEdge(idEdge3, "hasModerator", idVertex2, idVertex3);
    // create edge set
    Set<EPGMEdge> edges = new HashSet<>();
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
      "vddddddddddddddddddddddddaaaaaaaaaaaaaaaaaaaaaaaa [label=\"Person\",name=\"Anna\",age=\"27\"];\n" +
      "vbbbbbbbbbbbbbbbbbbbbbbbbaaaaaaaaaaaaaaaaaaaaaaaa [label=\"Person\",name=\"Tom\",age=\"25\"];\n" +
      "vccccccccccccccccccccccccaaaaaaaaaaaaaaaaaaaaaaaa [label=\"Forum\",lan=\"EN\"];\n" +
      "vddddddddddddddddddddddddaaaaaaaaaaaaaaaaaaaaaaaa->" +
            "vbbbbbbbbbbbbbbbbbbbbbbbbaaaaaaaaaaaaaaaaaaaaaaaa [label=\"knows\"];\n" +
      "vccccccccccccccccccccccccaaaaaaaaaaaaaaaaaaaaaaaa->" +
            "vddddddddddddddddddddddddaaaaaaaaaaaaaaaaaaaaaaaa [label=\"hasModerator\"];\n" +
      "vbbbbbbbbbbbbbbbbbbbbbbbbaaaaaaaaaaaaaaaaaaaaaaaa->" +
            "vddddddddddddddddddddddddaaaaaaaaaaaaaaaaaaaaaaaa [label=\"knows\"];\n" +
      "}\n";

    assertEquals(expected, dotFileFormat.format(transaction));
  }

  /**
   * Tests whether label and property value strings are being escaped according to java string rules.
   */
  @Test
  public void testStringEscaping() {
    EPGMGraphHead graphHead = new EPGMGraphHeadFactory()
            .initGraphHead(GradoopId.fromString("aaaaaaaaaaaaaaaaaaaaaaaa"), "graph");

    String key = "Title";
    String value = "Why was Wojo occasionally known to say \"I'm too lazy!\"?";

    // init vertex 1
    Map<String, Object> propertiesMap1 = new HashMap<>();
    propertiesMap1.put(key, value);
    GradoopId idVertex1 = GradoopId.fromString("bbbbbbbbbbbbbbbbbbbbbbbb");
    EPGMVertex vertex1 = new EPGMVertexFactory().initVertex(
            idVertex1, "weird\nlabe\"l", Properties.createFromMap(propertiesMap1));

    // create vertex set
    Set<EPGMVertex> vertices = new HashSet<>();
    vertices.add(vertex1);
    // create empty edge set
    Set<EPGMEdge> edges = new HashSet<>();

    GraphTransaction transactionMock = mock(GraphTransaction.class);
    when(transactionMock.getGraphHead()).thenReturn(graphHead);
    when(transactionMock.getVertices()).thenReturn(vertices);
    when(transactionMock.getEdges()).thenReturn(edges);

    DotFileFormatSimple dotFileFormatSimple = new DotFileFormatSimple(true);

    String expected = "subgraph cluster_gaaaaaaaaaaaaaaaaaaaaaaaa{\n" +
            "label=\"graph\";\n" +
            "vbbbbbbbbbbbbbbbbbbbbbbbbaaaaaaaaaaaaaaaaaaaaaaaa [label=\"weird\\nlabe\\\"l\"," +
            "Title=\"Why was Wojo occasionally known to say \\\"I'm too lazy!\\\"?\"];\n" +
            "}\n";

    assertEquals(expected, dotFileFormatSimple.format(transactionMock));
  }
}
