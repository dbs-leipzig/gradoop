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
package org.gradoop.flink.algorithms.fsm.dimspan.functions.conversion;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConfig;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConstants;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DataflowStep;
import org.gradoop.flink.algorithms.fsm.dimspan.model.GraphUtils;
import org.gradoop.flink.algorithms.fsm.dimspan.model.GraphUtilsBase;
import org.gradoop.flink.algorithms.fsm.dimspan.model.Simple16Compressor;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

import java.util.Set;

/**
 * int-array encoded graph => Gradoop Graph Transaction
 */
public class DFSCodeToEPGMGraphTransaction
  extends RichMapFunction<WithCount<int[]>, GraphTransaction> {


  /**
   * flag to enable decompression of patterns (true=enabled)
   */
  private final boolean uncompressPatterns;
  /**
   * frequent vertex labels
   */
  private String[] vertexDictionary;

  /**
   * frequent vertex labels
   */
  private String[] edgeDictionary;

  /**
   * utils to interpret and manipulate integer encoded graphs
   */
  private final GraphUtils graphUtils = new GraphUtilsBase();

  /**
   * Input graph collection size
   */
  private long graphCount;

  /**
   * Constructor.
   *
   * @param fsmConfig FSM configuration.
   */
  public DFSCodeToEPGMGraphTransaction(DIMSpanConfig fsmConfig) {
    this.uncompressPatterns =
      ! fsmConfig.getPatternCompressionInStep().equals(DataflowStep.WITHOUT);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    vertexDictionary = getRuntimeContext()
      .<String[]>getBroadcastVariable(DIMSpanConstants.VERTEX_DICTIONARY).get(0);

    edgeDictionary = getRuntimeContext()
      .<String[]>getBroadcastVariable(DIMSpanConstants.EDGE_DICTIONARY).get(0);


    graphCount = getRuntimeContext()
      .<Long>getBroadcastVariable(DIMSpanConstants.GRAPH_COUNT).get(0);
  }

  @Override
  public GraphTransaction map(WithCount<int[]> patternWithCount) throws Exception {

    int[] pattern = patternWithCount.getObject();

    if (uncompressPatterns) {
      pattern = Simple16Compressor.uncompress(pattern);
    }

    long frequency = patternWithCount.getCount();


    // GRAPH HEAD
    GraphHead graphHead = new GraphHead(GradoopId.get(), "", null);
    graphHead.setLabel(DIMSpanConstants.FREQUENT_PATTERN_LABEL);
    graphHead.setProperty(DIMSpanConstants.SUPPORT_KEY, (float) frequency / graphCount);

    GradoopIdSet graphIds = GradoopIdSet.fromExisting(graphHead.getId());

    // VERTICES
    int[] vertexLabels = graphUtils.getVertexLabels(pattern);

    GradoopId[] vertexIds = new GradoopId[vertexLabels.length];

    Set<Vertex> vertices = Sets.newHashSetWithExpectedSize(vertexLabels.length);

    int intId = 0;
    for (int intLabel : vertexLabels) {
      String label = vertexDictionary[intLabel];

      GradoopId gradoopId = GradoopId.get();
      vertices.add(new Vertex(gradoopId, label, null, graphIds));

      vertexIds[intId] = gradoopId;
      intId++;
    }

    // EDGES
    Set<Edge> edges = Sets.newHashSet();

    for (int edgeId = 0; edgeId < graphUtils.getEdgeCount(pattern); edgeId++) {
      String label = edgeDictionary[graphUtils.getEdgeLabel(pattern, edgeId)];

      GradoopId sourceId;
      GradoopId targetId;

      if (graphUtils.isOutgoing(pattern, edgeId)) {
        sourceId = vertexIds[graphUtils.getFromId(pattern, edgeId)];
        targetId = vertexIds[graphUtils.getToId(pattern, edgeId)];
      } else {
        sourceId = vertexIds[graphUtils.getToId(pattern, edgeId)];
        targetId = vertexIds[graphUtils.getFromId(pattern, edgeId)];
      }

      edges.add(new Edge(GradoopId.get(), label, sourceId, targetId, null, graphIds));
    }

    return new GraphTransaction(graphHead, vertices, edges);
  }
}
