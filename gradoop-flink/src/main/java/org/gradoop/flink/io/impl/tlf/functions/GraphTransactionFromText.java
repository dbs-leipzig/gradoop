/**
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
package org.gradoop.flink.io.impl.tlf.functions;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.flink.io.impl.tlf.TLFConstants;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

import java.util.Map;
import java.util.Set;

/**
 * Reads graph imported from a TLF file. The result of the mapping is a
 * dataset of of tlf graphs, with each TLFGraph consisting of a tlf graph
 * head, a collection of tlf vertices and a collection of tlf edges.
 */
public class GraphTransactionFromText
  implements MapFunction<Tuple2<LongWritable, Text>, GraphTransaction> {

  /**
   * Graph head factory.
   */
  private GraphHeadFactory graphHeadFactory;
  /**
   * Vertex factory.
   */
  private VertexFactory vertexFactory;
  /**
   * Edge factory.
   */
  private EdgeFactory edgeFactory;
  /**
   * Line fields splittet by space.
   */
  private String[] fields;
  /**
   * String builder for each line.
   */
  private StringBuilder stringBuilder = new StringBuilder();
  /**
   * String builder for label with spaces.
   */
  private StringBuilder labelBuilder = new StringBuilder();
  /**
   * Map for long id from tlf file to gradoop id.
   */
  private Map<Long, GradoopId> idMap = Maps.newHashMap();
  /**
   * Vertices of transaction.
   */
  private Set<Vertex> vertices = Sets.newHashSet();
  /**
   * Edges of transaction.
   */
  private Set<Edge> edges = Sets.newHashSet();

  /**
   * Valued constructor.
   *
   * @param graphHeadFactory graph head factory
   * @param vertexFactory vertex factory
   * @param edgeFactory edge factory
   */
  public GraphTransactionFromText(GraphHeadFactory graphHeadFactory, VertexFactory vertexFactory,
    EdgeFactory edgeFactory) {
    this.graphHeadFactory = graphHeadFactory;
    this.vertexFactory = vertexFactory;
    this.edgeFactory = edgeFactory;
  }

  /**
   * Cunstructs a dataset containing TLFGraph(s).
   *
   * @param inputTuple consists of a key(LongWritable) and a value(Text)
   * @return a TLFGraph created by the input text
   * @throws Exception
   */
  @Override
  public GraphTransaction map(Tuple2<LongWritable, Text> inputTuple) throws Exception {
    idMap.clear();
    vertices.clear();
    edges.clear();
    stringBuilder.setLength(0);
    boolean firstLine = true;
    boolean vertexLine = true;
    String graph = inputTuple.f1.toString();
    int cursor = 0;
    char currChar;

    GradoopId gradoopId;
    GraphHead graphHead = null;
    Vertex vertex;
    Edge edge;

    do {
      currChar = graph.charAt(cursor);
      if (currChar == '\n') {
        fields = stringBuilder.toString().trim().split(" ");
        if (firstLine) {
          gradoopId = GradoopId.get();
          idMap.put(Long.valueOf(fields[2]), gradoopId);
          graphHead = graphHeadFactory.initGraphHead(gradoopId);
          firstLine = false;
        } else {
          if (vertexLine) {
            gradoopId = GradoopId.get();
            idMap.put(Long.valueOf(fields[1]), gradoopId);
            vertex = vertexFactory.initVertex(gradoopId, getLabel(2));
            vertex.addGraphId(graphHead.getId());
            vertices.add(vertex);
            if (TLFConstants.EDGE_SYMBOL.equals(String.valueOf(graph.charAt(cursor + 1)))) {
              vertexLine = false;
            }
          } else {
            gradoopId = GradoopId.get();
            edge = edgeFactory.initEdge(gradoopId,
              getLabel(3),
              idMap.get(Long.valueOf(fields[1])),
              idMap.get(Long.valueOf(fields[2]))
            );
            edge.addGraphId(graphHead.getId());
            edges.add(edge);
          }
        }
        stringBuilder.setLength(0);
      } else {
        stringBuilder.append(currChar);
      }
      cursor++;
    } while (cursor != graph.length());

    return new GraphTransaction(graphHead, vertices, edges);
  }

  /**
   * Builds a label from the current fields. If the label is split by whitespaces the last fields
   * which represent the label will be concatenated.
   *
   * @param labelStart field where the label starts
   * @return full label
   */
  private String getLabel(int labelStart) {
    labelBuilder.setLength(0);
    for (int i = labelStart; i < fields.length; i++) {
      if (i > labelStart) {
        labelBuilder.append(" ");
      }
      labelBuilder.append(fields[i]);
    }
    return labelBuilder.toString();
  }
}
