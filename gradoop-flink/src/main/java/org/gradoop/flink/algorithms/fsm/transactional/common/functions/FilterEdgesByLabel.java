
package org.gradoop.flink.algorithms.fsm.transactional.common.functions;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.algorithms.fsm.transactional.common.TFSMConstants;
import org.gradoop.flink.representation.transactional.GraphTransaction;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * Drops edges with infrequent labels and isolated vertices.
 */
public class FilterEdgesByLabel extends RichMapFunction<GraphTransaction, GraphTransaction> {

  /**
   * frequent edge labels
   */
  private Collection<String> frequentEdgeLabels;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    Collection<String> broadcast = getRuntimeContext()
      .getBroadcastVariable(TFSMConstants.FREQUENT_EDGE_LABELS);

    this.frequentEdgeLabels = Sets.newHashSet(broadcast);
  }

  @Override
  public GraphTransaction map(GraphTransaction transaction) throws Exception {

    Set<GradoopId> referenceEdgeIds = Sets.newHashSet();

    // drop edges with infrequent labels

    Iterator<Edge> edgeIterator = transaction.getEdges().iterator();

    while (edgeIterator.hasNext()) {
      Edge next = edgeIterator.next();

      if (frequentEdgeLabels.contains(next.getLabel())) {
        referenceEdgeIds.add(next.getSourceId());
        referenceEdgeIds.add(next.getTargetId());
      } else {
        edgeIterator.remove();
      }
    }

    // drop vertex without any edges

    transaction.getVertices().removeIf(next -> !referenceEdgeIds.contains(next.getId()));

    return transaction;
  }
}
