package org.gradoop.flink.algorithms.fsm.transactional.common.functions;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.fsm.transactional.tle.common.config.Constants;
import org.gradoop.flink.representation.transactional.sets.GraphTransaction;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

public class FilterVerticesByLabel
  extends RichMapFunction<GraphTransaction, GraphTransaction> {

  /**
   * frequent vertex labels
   */
  private Collection<String> frequentVertexLabels;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    Collection<String> broadcast = getRuntimeContext()
      .getBroadcastVariable(Constants.FREQUENT_VERTEX_LABELS);

    this.frequentVertexLabels = Sets.newHashSet(broadcast);
  }

  @Override
  public GraphTransaction map(GraphTransaction transaction) throws Exception {

    Set<GradoopId> keptVertexIds = Sets.newHashSet();

    // drop vertices with infrequent labels

    Iterator<Vertex> vertexIterator = transaction.getVertices().iterator();

    while (vertexIterator.hasNext()) {
      Vertex next = vertexIterator.next();

      if (frequentVertexLabels.contains(next.getLabel())) {
        keptVertexIds.add(next.getId());
      } else {
        vertexIterator.remove();
      }
    }

    // drop inconsistent edges

    Iterator<Edge> edgeIterator = transaction.getEdges().iterator();

    while (edgeIterator.hasNext()) {
      Edge next = edgeIterator.next();

      if (! keptVertexIds.contains(next.getSourceId()) ||
        ! keptVertexIds.contains(next.getTargetId())) {
        edgeIterator.remove();
      }
    }

    return transaction;
  }
}
