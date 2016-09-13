package org.gradoop.flink.algorithms.fsm.functions;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.fsm.config.Constants;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

public class DropEdgesWithInfrequentLabels extends
  RichMapFunction<GraphTransaction, GraphTransaction> {

  private Collection<String> frequentEdgeLabels;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    this.frequentEdgeLabels = getRuntimeContext()
      .getBroadcastVariable(Constants.EDGE_DICTIONARY);

    this.frequentEdgeLabels = Sets.newHashSet(frequentEdgeLabels);
  }


  @Override
  public GraphTransaction map(GraphTransaction value) throws Exception {

    Set<GradoopId> connectedVertexIds = Sets.newHashSet();

    Iterator<Edge> edgeIterator = value.getEdges().iterator();

    while (edgeIterator.hasNext()) {
      Edge edge = edgeIterator.next();

      if (frequentEdgeLabels.contains(edge.getLabel())) {
        connectedVertexIds.add(edge.getSourceId());
        connectedVertexIds.add(edge.getTargetId());
      } else {
        edgeIterator.remove();
      }
    }

    Iterator<Vertex> vertexIterator = value.getVertices().iterator();

    while (vertexIterator.hasNext()) {
      Vertex vertex = vertexIterator.next();

      if (! connectedVertexIds.contains(vertex.getId())) {
        vertexIterator.remove();
      }
    }

    return value;
  }
}
