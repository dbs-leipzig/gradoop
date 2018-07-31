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
package org.gradoop.flink.model.impl.operators.matching.common.functions;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
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
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.query.Step;
import org.gradoop.flink.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.PatternMatching;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Extracts {@link Element} instances from an {@link Embedding}.
 */
public class ElementsFromEmbedding
  extends RichFlatMapFunction<Tuple1<Embedding<GradoopId>>, Element> {

  /**
   * Maps edge candidates to the step in which they are traversed
   */
  private final Map<Integer, Step> edgeToStep;
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
   * Maps query vertex ids to variables
   */
  private final Map<Long, String> queryVertexMapping;
  /**
   * Maps query edge ids to variables
   */
  private final Map<Long, String> queryEdgeMapping;
  /**
   * Reuse map for storing the variable mapping
   */
  private Map<PropertyValue, PropertyValue> reuseVariableMapping;
  /**
   * Constructor
   *
   * @param traversalCode     traversal code to retrieve sourceId/targetId
   * @param epgmGraphHeadFactory  EPGM graph head factory
   * @param epgmVertexFactory     EPGM vertex factory
   * @param epgmEdgeFactory       EPGM edge factory
   * @param query             query handler
   */
  public ElementsFromEmbedding(TraversalCode traversalCode,
    EPGMGraphHeadFactory<GraphHead> epgmGraphHeadFactory,
    EPGMVertexFactory<Vertex> epgmVertexFactory,
    EPGMEdgeFactory<Edge> epgmEdgeFactory,
    QueryHandler query) {
    this.graphHeadFactory = epgmGraphHeadFactory;
    this.vertexFactory = epgmVertexFactory;
    this.edgeFactory = epgmEdgeFactory;

    this.queryVertexMapping = query.getVertices()
      .stream()
      .collect(Collectors.toMap(v -> v.getId(), v -> v.getVariable()));

    this.queryEdgeMapping = query.getEdges()
      .stream()
      .collect(Collectors.toMap(e -> e.getId(), e -> e.getVariable()));

    List<Step> steps = traversalCode.getSteps();
    edgeToStep = Maps.newHashMapWithExpectedSize(steps.size());

    for (Step step : steps) {
      edgeToStep.put((int) step.getVia(), step);
    }
  }

  @Override
  public void open(Configuration conf) {
    this.reuseVariableMapping = new HashMap<>();
  }

  @Override
  public void flatMap(Tuple1<Embedding<GradoopId>> embedding, Collector<Element> out)
      throws Exception {

    GradoopId[] vertexMapping = embedding.f0.getVertexMapping();
    GradoopId[] edgeMapping = embedding.f0.getEdgeMapping();


    // create graph head for this embedding
    GraphHead graphHead = graphHeadFactory.createGraphHead();

    // collect vertices (and assign to graph head)
    for (int i = 0; i < vertexMapping.length; i++) {
      if (!isProcessed(vertexMapping, i)) {
        Vertex v = vertexFactory.initVertex(vertexMapping[i]);
        v.addGraphId(graphHead.getId());
        out.collect(v);
      }

      reuseVariableMapping.put(
        PropertyValue.create(queryVertexMapping.get((long) i)),
        PropertyValue.create(vertexMapping[i])
      );
    }

    // collect edges (and assign to graph head)
    for (int i = 0; i < edgeMapping.length; i++) {
      if (!isProcessed(edgeMapping, i)) {
        Step s = edgeToStep.get(i);
        // get sourceId/targetId according to traversal step
        GradoopId sourceId = s.isOutgoing() ?
          vertexMapping[(int) s.getFrom()] : vertexMapping[(int) s.getTo()];
        GradoopId targetId = s.isOutgoing() ?
          vertexMapping[(int) s.getTo()] : vertexMapping[(int) s.getFrom()];
        Edge e = edgeFactory.initEdge(edgeMapping[i], sourceId, targetId);
        e.addGraphId(graphHead.getId());
        out.collect(e);
      }

      reuseVariableMapping.put(
        PropertyValue.create(queryEdgeMapping.get((long) i)),
        PropertyValue.create(edgeMapping[i])
      );
    }

    graphHead.setProperty(PatternMatching.VARIABLE_MAPPING_KEY, reuseVariableMapping);
    out.collect(graphHead);
  }

  /**
   * Checks if the the id at the specified index has been processed before.
   *
   * @param mapping id mapping
   * @param i index
   * @return true, iff the element at position i is present at a position between 0 and i
   */
  private boolean isProcessed(GradoopId[] mapping, int i) {
    for (int j = 0; j < i; j++) {
      if (mapping[j].equals(mapping[i])) {
        return true;
      }
    }
    return false;
  }
}
