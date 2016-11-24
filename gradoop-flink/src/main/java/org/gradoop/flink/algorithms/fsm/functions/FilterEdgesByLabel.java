package org.gradoop.flink.algorithms.fsm.functions;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.fsm_old.common.config.Constants;
import org.gradoop.flink.representation.transactional.sets.GraphTransaction;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

public class FilterEdgesByLabel
  extends RichMapFunction<GraphTransaction, GraphTransaction> {

  /**
   * frequent edge labels
   */
  private Collection<String> frequentEdgeLabels;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    
    Collection<String> broadcast = getRuntimeContext()
      .getBroadcastVariable(Constants.FREQUENT_EDGE_LABELS);

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

    Iterator<Vertex> vertexIterator = transaction.getVertices().iterator();
    
    while (vertexIterator.hasNext()) {
      Vertex next = vertexIterator.next();
      
      if (! referenceEdgeIds.contains(next.getId())) {
        vertexIterator.remove();
      }
    }

    return transaction;
  }
}
