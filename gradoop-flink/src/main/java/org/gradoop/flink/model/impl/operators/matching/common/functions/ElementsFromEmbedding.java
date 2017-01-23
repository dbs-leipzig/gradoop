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

package org.gradoop.flink.model.impl.operators.matching.common.functions;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
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
   * @param graphHeadFactory  EPGM graph head factory
   * @param vertexFactory     EPGM vertex factory
   * @param edgeFactory       EPGM edge factory
   * @param query             query handler
   */
  public ElementsFromEmbedding(TraversalCode traversalCode,
    GraphHeadFactory graphHeadFactory,
    VertexFactory vertexFactory,
    EdgeFactory edgeFactory,
    QueryHandler query) {
    this.graphHeadFactory = graphHeadFactory;
    this.vertexFactory = vertexFactory;
    this.edgeFactory = edgeFactory;

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
