/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
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
public class ElementsFromEmbedding implements FlatMapFunction<Embedding, Element> {
  /**
   * Constructs EPGM graph heads
   */
  private final EPGMGraphHeadFactory<GraphHead> graphHeadFactory;
  /**
   * Constructs EPGM vertices
   */
  private final EPGMVertexFactory<Vertex> vertexFactory;
  /**
   * Constructs EPGM edges
   */
  private final EPGMEdgeFactory<Edge> edgeFactory;
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
   * Constructor.
   *
   * @param epgmGraphHeadFactory EPGM graph head factory
   * @param epgmVertexFactory EPGM vertex factory
   * @param epgmEdgeFactory EPGM edge factory
   * @param embeddingMetaData meta data for the embedding
   * @param sourceTargetVariables source and target vertex variables by edge variable
   */
  public ElementsFromEmbedding(EPGMGraphHeadFactory<GraphHead> epgmGraphHeadFactory,
    EPGMVertexFactory<Vertex> epgmVertexFactory,
    EPGMEdgeFactory<Edge> epgmEdgeFactory, EmbeddingMetaData embeddingMetaData,
    Map<String, Pair<String, String>> sourceTargetVariables) {
    this.graphHeadFactory = epgmGraphHeadFactory;
    this.vertexFactory = epgmVertexFactory;
    this.edgeFactory = epgmEdgeFactory;
    this.metaData = embeddingMetaData;
    this.sourceTargetVariables = sourceTargetVariables;
    this.variableMapping = new HashMap<>(embeddingMetaData.getEntryCount());
    this.processedIds = new HashSet<>(embeddingMetaData.getEntryCount());
  }

  @Override
  public void flatMap(Embedding embedding, Collector<Element> out) throws Exception {
    // clear for each embedding
    processedIds.clear();

    // create graph head for this embedding
    GraphHead graphHead = graphHeadFactory.createGraphHead();

    // vertices
    for (String vertexVariable : metaData.getVertexVariables()) {
      GradoopId id = embedding.getId(metaData.getEntryColumn(vertexVariable));
      initVertex(out, graphHead, id);
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

      initEdge(out, graphHead, edgeId, sourceId, targetId);
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
  private void initVertex(Collector<Element> out, GraphHead graphHead, GradoopId vertexId) {
    if (!processedIds.contains(vertexId)) {
      Vertex v = vertexFactory.initVertex(vertexId);
      v.addGraphId(graphHead.getId());
      out.collect(v);
      processedIds.add(vertexId);
    }
  }

  /**
   * Initializes an EPGM edge using the speciified parameters.
   *
   * @param out flat map collector
   * @param graphHead graph head to assign edge to
   * @param edgeId edge identifier
   * @param sourceId source vertex identifier
   * @param targetId target vertex identifier
   */
  private void initEdge(Collector<Element> out, GraphHead graphHead, GradoopId edgeId,
    GradoopId sourceId, GradoopId targetId) {
    if (!processedIds.contains(edgeId)) {
      Edge e = edgeFactory.initEdge(edgeId, sourceId, targetId);
      e.addGraphId(graphHead.getId());
      out.collect(e);
      processedIds.add(edgeId);
    }
  }
}
