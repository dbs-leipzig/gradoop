package org.gradoop.model.impl.operators.matching.common.functions;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.api.EPGMElement;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMGraphHeadFactory;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.matching.common.query.Step;
import org.gradoop.model.impl.operators.matching.common.query.TraversalCode;
import org.gradoop.model.impl.operators.matching.common.tuples.Embedding;

import java.util.List;
import java.util.Map;

/**
 * Copyright 2016 martin.
 */
public class ElementsFromEmbedding
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements FlatMapFunction<Tuple1<Embedding>, EPGMElement> {

  private final Map<Integer, Step> edgeToStep;

  private final EPGMGraphHeadFactory<G> graphHeadFactory;
  private final EPGMVertexFactory<V> vertexFactory;
  private final EPGMEdgeFactory<E> edgeFactory;

  public ElementsFromEmbedding(TraversalCode traversalCode,
    EPGMGraphHeadFactory<G> graphHeadFactory,
    EPGMVertexFactory<V> vertexFactory,
    EPGMEdgeFactory<E> edgeFactory) {
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
  public void flatMap(Tuple1<Embedding> embedding, Collector<EPGMElement> out) throws Exception {
    GradoopId[] vertexEmbeddings  = embedding.f0.getVertexEmbeddings();
    GradoopId[] edgeEmbeddings    = embedding.f0.getEdgeEmbeddings();

    // create graph head
    G graphHead = graphHeadFactory.createGraphHead();
    out.collect(graphHead);

    // collect vertices
    for (GradoopId vertexId : vertexEmbeddings) {
      V v = vertexFactory.initVertex(vertexId);
      v.addGraphId(graphHead.getId());
      out.collect(v);
    }

    // collect edges
    for (int i = 0; i < edgeEmbeddings.length; i++) {
      Step s = edgeToStep.get(i);
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
