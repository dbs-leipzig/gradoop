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
package org.gradoop.flink.io.impl.tlf.functions;

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
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
   * Valued constructor.
   *
   * @param config gradoop flink config
   */
  public GraphTransactionFromText(GradoopFlinkConfig config) {
    this.graphHeadFactory = config.getGraphHeadFactory();
    this.vertexFactory = config.getVertexFactory();
    this.edgeFactory = config.getEdgeFactory();
  }

  /**
   * Cunstructs a dataset containing TLFGraph(s).
   *
   * @param inputTuple consists of a key(LongWritable) and a value(Text)
   * @return a TLFGraph created by the input text
   * @throws Exception on failure
   */
  @Override
  public GraphTransaction map(Tuple2<LongWritable, Text> inputTuple) throws Exception {
    Map<Long, GradoopId> idMap = new HashMap<>();
    Set<Vertex> vertices = new HashSet<>();
    Set<Edge> edges = new HashSet<>();
    GraphHead graphHead = null;

    String[] lines = inputTuple.f1.toString().split("\\R", -1);
    for (int i = 0; i < lines.length; i++) {
      String[] fields = lines[i].trim().split(" ");
      GradoopId gradoopId = GradoopId.get();

      if (i == 0) {
        idMap.put(Long.valueOf(fields[2]), gradoopId);
        graphHead = graphHeadFactory.initGraphHead(gradoopId);

      } else if (TLFConstants.VERTEX_SYMBOL.equals(fields[0])) {
        idMap.put(Long.valueOf(fields[1]), gradoopId);
        Vertex vertex = vertexFactory.initVertex(gradoopId, getLabel(fields, 2));
        vertex.addGraphId(graphHead.getId());
        vertices.add(vertex);

      } else if (TLFConstants.EDGE_SYMBOL.equals(fields[0])) {
        Edge edge = edgeFactory.initEdge(gradoopId,
          getLabel(fields, 3),
          idMap.get(Long.valueOf(fields[1])),
          idMap.get(Long.valueOf(fields[2]))
        );
        edge.addGraphId(graphHead.getId());
        edges.add(edge);
      }
    }

    return new GraphTransaction(graphHead, vertices, edges);
  }

  /**
   * Builds a label from the current fields. If the label is split by whitespaces the last fields
   * which represent the label will be concatenated.
   *
   * @param fields graph element fields
   * @param labelStart field where the label starts
   * @return full label
   */
  private String getLabel(String[] fields, int labelStart) {
    return Arrays.stream(fields).skip(labelStart).collect(Collectors.joining(" "));
  }
}
