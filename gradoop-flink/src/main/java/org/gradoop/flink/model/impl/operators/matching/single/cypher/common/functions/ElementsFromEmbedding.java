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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.common.functions;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.PatternMatching;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Extracts EPGM elements from an {@link Embedding}.
 */
public class ElementsFromEmbedding implements FlatMapFunction<Embedding, Element> {
  /**
   * Constructs EPGM graph heads
   */
  private final GraphHeadFactory graphHeadFactory;
  /**
   * Constructs EPGM vertices
   */
  private final VertexFactory vertexFactory;
  /**
   * Constructs EPGM edges
   */
  private final EdgeFactory edgeFactory;
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
   * @param graphHeadFactory EPGM graph head factory
   * @param vertexFactory EPGM vertex factory
   * @param edgeFactory EPGM edge factory
   * @param embeddingMetaData meta data for the embedding
   * @param sourceTargetVariables source and target vertex variables by edge variable
   */
  public ElementsFromEmbedding(GraphHeadFactory graphHeadFactory, VertexFactory vertexFactory,
    EdgeFactory edgeFactory, EmbeddingMetaData embeddingMetaData,
    Map<String, Pair<String, String>> sourceTargetVariables) {
    this.graphHeadFactory = graphHeadFactory;
    this.vertexFactory = vertexFactory;
    this.edgeFactory = edgeFactory;
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
      if (!processedIds.contains(id)) {
        Vertex v = vertexFactory.initVertex(id);
        v.addGraphId(graphHead.getId());
        out.collect(v);
        processedIds.add(id);
      }
      variableMapping.put(PropertyValue.create(vertexVariable), PropertyValue.create(id));
    }

    // edges
    for (String edgeVariable : metaData.getEdgeVariables()) {
      if (metaData.getEntryType(edgeVariable) == EmbeddingMetaData.EntryType.EDGE) {
        GradoopId edgeId = embedding.getId(metaData.getEntryColumn(edgeVariable));
        if (!processedIds.contains(edgeId)) {
          GradoopId sourceId = embedding.getId(
            metaData.getEntryColumn(sourceTargetVariables.get(edgeVariable).getLeft()));
          GradoopId targetId = embedding.getId(
            metaData.getEntryColumn(sourceTargetVariables.get(edgeVariable).getRight()));
          Edge e = edgeFactory.initEdge(edgeId, sourceId, targetId);
          e.addGraphId(graphHead.getId());
          out.collect(e);
          processedIds.add(edgeId);
        }
        variableMapping.put(PropertyValue.create(edgeVariable), PropertyValue.create(edgeId));
      }
    }

    graphHead.setProperty(PatternMatching.VARIABLE_MAPPING_KEY, variableMapping);
    out.collect(graphHead);
  }
}
