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

package org.gradoop.model.impl.operators.matching.common.functions;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.epgm.Edge;
import org.gradoop.model.api.epgm.EdgeFactory;
import org.gradoop.model.api.epgm.Element;
import org.gradoop.model.api.epgm.GraphHead;
import org.gradoop.model.api.epgm.GraphHeadFactory;
import org.gradoop.model.api.epgm.Vertex;
import org.gradoop.model.api.epgm.VertexFactory;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.matching.common.query.Step;
import org.gradoop.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.model.impl.operators.matching.common.tuples.Embedding;

import java.util.List;
import java.util.Map;

/**
 * Extracts {@link Element} instances from an {@link Embedding}.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class ElementsFromEmbedding
  <G extends GraphHead, V extends Vertex, E extends Edge>
  implements FlatMapFunction<Tuple1<Embedding>, Element> {

  /**
   * Maps edge candidates to the step in which they are traversed
   */
  private final Map<Integer, Step> edgeToStep;
  /**
   * Constructs EPGM graph heads
   */
  private final GraphHeadFactory<G> graphHeadFactory;
  /**
   * Constructs EPGM vertices
   */
  private final VertexFactory<V> vertexFactory;
  /**
   * Constructs EPGM edges
   */
  private final EdgeFactory<E> edgeFactory;

  /**
   * Constructor
   *
   * @param traversalCode     traversal code to retrieve sourceId/targetId
   * @param graphHeadFactory  EPGM graph head factory
   * @param vertexFactory     EPGM vertex factory
   * @param edgeFactory       EPGM edge factory
   */
  public ElementsFromEmbedding(TraversalCode traversalCode,
    GraphHeadFactory<G> graphHeadFactory,
    VertexFactory<V> vertexFactory,
    EdgeFactory<E> edgeFactory) {
    this.graphHeadFactory = graphHeadFactory;
    this.vertexFactory = vertexFactory;
    this.edgeFactory = edgeFactory;

    List<Step> steps    = traversalCode.getSteps();
    edgeToStep          = Maps.newHashMapWithExpectedSize(steps.size());
    for (Step step : steps) {
      edgeToStep.put((int) step.getVia(), step);
    }
  }

  @Override
  public void flatMap(Tuple1<Embedding> embedding, Collector<Element> out)
      throws Exception {
    GradoopId[] vertexEmbeddings  = embedding.f0.getVertexMappings();
    GradoopId[] edgeEmbeddings    = embedding.f0.getEdgeMappings();

    // create graph head
    G graphHead = graphHeadFactory.createGraphHead();
    out.collect(graphHead);

    // collect vertices (and assign to graph head)
    for (GradoopId vertexId : vertexEmbeddings) {
      V v = vertexFactory.initVertex(vertexId);
      v.addGraphId(graphHead.getId());
      out.collect(v);
    }

    // collect edges (and assign to graph head)
    for (int i = 0; i < edgeEmbeddings.length; i++) {
      Step s = edgeToStep.get(i);
      // get sourceId/targetId according to traversal step
      GradoopId sourceId = s.isOutgoing() ?
        vertexEmbeddings[(int) s.getFrom()] : vertexEmbeddings[(int) s.getTo()];
      GradoopId targetId = s.isOutgoing() ?
        vertexEmbeddings[(int) s.getTo()] : vertexEmbeddings[(int) s.getFrom()];
      E e = edgeFactory.initEdge(edgeEmbeddings[i], sourceId, targetId);
      e.addGraphId(graphHead.getId());
      out.collect(e);
    }
  }
}
