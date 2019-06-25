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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.functions;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EdgeFactory;
import org.gradoop.common.model.api.entities.GraphHeadFactory;
import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMElement;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.PatternMatching;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.ExpandDirection;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Extracts EPGM elements from an {@link Embedding}.
 */
public class ElementsFromEmbedding implements FlatMapFunction<Embedding, EPGMElement> {
  /**
   * Constructs EPGM graph heads
   */
  private final GraphHeadFactory<EPGMGraphHead> graphHeadFactory;
  /**
   * Constructs EPGM vertices
   */
  private final VertexFactory<EPGMVertex> vertexFactory;
  /**
   * Constructs EPGM edges
   */
  private final EdgeFactory<EPGMEdge> edgeFactory;
  /**
   * Describes the embedding content
   */
  private final EmbeddingMetaData metaData;
  /**
   * Source vertex variables by edge id
   */
  private final Map<String, Pair<String, String>> sourceTargetVariables;
  /**
   * Stores the mapping between query variable and element id. The mapping is added as a property
   * to the graph head representing an embedding.
   */
  private final Map<PropertyValue, PropertyValue> variableMapping;
  /**
   * Stores the identifiers that have already been processed.
   */
  private final Set<GradoopId> processedIds;
  /**
   * Stores the mapping between return pattern variables and its labels
   */
  private final Map<String, String> labelMapping;

  /**
   * Constructor.
   *
   * @param epgmGraphHeadFactory EPGM graph head factory
   * @param epgmVertexFactory EPGM vertex factory
   * @param epgmEdgeFactory EPGM edge factory
   * @param embeddingMetaData meta data for the embedding
*    @param sourceTargetVariables source and target vertex variables by edge variable
   */
  public ElementsFromEmbedding(GraphHeadFactory<EPGMGraphHead> epgmGraphHeadFactory,
    VertexFactory<EPGMVertex> epgmVertexFactory,
    EdgeFactory<EPGMEdge> epgmEdgeFactory, EmbeddingMetaData embeddingMetaData,
    Map<String, Pair<String, String>> sourceTargetVariables) {
    this(epgmGraphHeadFactory, epgmVertexFactory, epgmEdgeFactory, embeddingMetaData,
      sourceTargetVariables, Maps.newHashMapWithExpectedSize(0));
  }
  /**
   * Constructor.
   *
   * @param epgmGraphHeadFactory EPGM graph head factory
   * @param epgmVertexFactory EPGM vertex factory
   * @param epgmEdgeFactory EPGM edge factory
   * @param embeddingMetaData meta data for the embedding
   * @param sourceTargetVariables source and target vertex variables by edge variable
   * @param labelMapping mapping between newElementVariables and its labels
   */
  public ElementsFromEmbedding(GraphHeadFactory<EPGMGraphHead> epgmGraphHeadFactory,
    VertexFactory<EPGMVertex> epgmVertexFactory,
    EdgeFactory<EPGMEdge> epgmEdgeFactory, EmbeddingMetaData embeddingMetaData,
    Map<String, Pair<String, String>> sourceTargetVariables,
    Map<String, String> labelMapping) {
    this.graphHeadFactory = epgmGraphHeadFactory;
    this.vertexFactory = epgmVertexFactory;
    this.edgeFactory = epgmEdgeFactory;
    this.metaData = embeddingMetaData;
    this.sourceTargetVariables = sourceTargetVariables;
    this.labelMapping = labelMapping;
    this.variableMapping = new HashMap<>(embeddingMetaData.getEntryCount());
    this.processedIds = new HashSet<>(embeddingMetaData.getEntryCount());
  }

  @Override
  public void flatMap(Embedding embedding, Collector<EPGMElement> out) throws Exception {
    // clear for each embedding
    processedIds.clear();

    // create graph head for this embedding
    EPGMGraphHead graphHead = graphHeadFactory.createGraphHead();

    // vertices
    for (String vertexVariable : metaData.getVertexVariables()) {
      GradoopId id = embedding.getId(metaData.getEntryColumn(vertexVariable));

      if (labelMapping.containsKey(vertexVariable)) {
        String label = labelMapping.get(vertexVariable);
        initVertexWithData(out, graphHead, id, label);
      } else {
        initVertex(out, graphHead, id);
      }
      variableMapping.put(PropertyValue.create(vertexVariable), PropertyValue.create(id));
    }

    // edges
    GradoopId edgeId;
    GradoopId sourceId;
    GradoopId targetId;
    for (String edgeVariable : metaData.getEdgeVariables()) {
      edgeId = embedding.getId(metaData.getEntryColumn(edgeVariable));
      sourceId = embedding.getId(
        metaData.getEntryColumn(sourceTargetVariables.get(edgeVariable).getLeft()));
      targetId = embedding.getId(
        metaData.getEntryColumn(sourceTargetVariables.get(edgeVariable).getRight()));

      if (labelMapping.containsKey(edgeVariable)) {
        String label = labelMapping.get(edgeVariable);
        initEdgeWithData(out, graphHead, edgeId, sourceId, targetId, label);
      } else {
        initEdge(out, graphHead, edgeId, sourceId, targetId);
      }
      variableMapping.put(PropertyValue.create(edgeVariable), PropertyValue.create(edgeId));
    }

    // paths
    for (String pathVariable : metaData.getPathVariables()) {
      ExpandDirection direction = metaData.getDirection(pathVariable);
      List<GradoopId> path = embedding.getIdList(metaData.getEntryColumn(pathVariable));
      List<PropertyValue> mappingValue = new ArrayList<>(path.size());
      for (int i = 0; i < path.size(); i += 2) {
        edgeId = path.get(i);
        mappingValue.add(PropertyValue.create(edgeId));

        if (direction == ExpandDirection.OUT) {
          sourceId = i > 0 ?
            path.get(i - 1) :
            embedding.getId(
              metaData.getEntryColumn(sourceTargetVariables.get(pathVariable).getLeft()));

          targetId = i < path.size() - 1 ?
            path.get(i + 1) :
            embedding.getId(
              metaData.getEntryColumn(sourceTargetVariables.get(pathVariable).getRight()));

          if (i + 2 < path.size()) {
            mappingValue.add(PropertyValue.create(targetId));
          }
        } else {
          sourceId = i < path.size() - 1 ?
            path.get(i + 1) :
            embedding.getId(
              metaData.getEntryColumn(sourceTargetVariables.get(pathVariable).getLeft()));

          targetId = i > 0 ?
            path.get(i - 1) :
            embedding.getId(
              metaData.getEntryColumn(sourceTargetVariables.get(pathVariable).getRight()));

          if (i > 0) {
            mappingValue.add(PropertyValue.create(sourceId));
          }
        }

        initVertex(out, graphHead, sourceId);
        initVertex(out, graphHead, targetId);
        initEdge(out, graphHead, edgeId, sourceId, targetId);
      }
      variableMapping.put(PropertyValue.create(pathVariable), PropertyValue.create(mappingValue));
    }

    graphHead.setProperty(PatternMatching.VARIABLE_MAPPING_KEY, variableMapping);
    out.collect(graphHead);
  }

  /**
   * Initializes an EPGM vertex using the specified parameters
   *
   * @param out flat map collector
   * @param graphHead graph head to assign vertex to
   * @param vertexId vertex identifier
   */
  private void initVertex(Collector<EPGMElement> out, EPGMGraphHead graphHead, GradoopId vertexId) {
    initVertexWithData(out, graphHead, vertexId, null);
  }

  /**
   * Initializes an EPGM vertex using the specified parameters and adds its label
   * if the given vertex was created for the return pattern.
   *
   * @param out flat map collector
   * @param graphHead graph head to assign vertex to
   * @param vertexId vertex identifier
   * @param label label associated with vertex
   */
  private void initVertexWithData(Collector<EPGMElement> out, EPGMGraphHead graphHead, GradoopId vertexId,
                                  String label) {
    if (!processedIds.contains(vertexId)) {
      EPGMVertex v = vertexFactory.initVertex(vertexId);
      v.addGraphId(graphHead.getId());
      v.setLabel(label);
      out.collect(v);
      processedIds.add(vertexId);
    }
  }

  /**
   * Initializes an EPGM edge using the specified parameters.
   *
   * @param out flat map collector
   * @param graphHead graph head to assign edge to
   * @param edgeId edge identifier
   * @param sourceId source vertex identifier
   * @param targetId target vertex identifier
   */
  private void initEdge(Collector<EPGMElement> out, EPGMGraphHead graphHead, GradoopId edgeId,
    GradoopId sourceId, GradoopId targetId) {
    initEdgeWithData(out, graphHead, edgeId, sourceId, targetId, null);
  }

  /**
   * Initializes an EPGM edge using the specified parameters and adds its label
   * if the given edge was created for return pattern
   *
   * @param out flat map collector
   * @param graphHead graph head to assign edge to
   * @param edgeId edge identifier
   * @param sourceId source vertex identifier
   * @param targetId target vertex identifier
   * @param label label associated with edge
   */
  private void initEdgeWithData(Collector<EPGMElement> out, EPGMGraphHead graphHead, GradoopId edgeId,
                                GradoopId sourceId, GradoopId targetId, String label) {
    if (!processedIds.contains(edgeId)) {
      EPGMEdge e = edgeFactory.initEdge(edgeId, sourceId, targetId);
      e.addGraphId(graphHead.getId());
      e.setLabel(label);
      out.collect(e);
      processedIds.add(edgeId);
    }
  }
}
