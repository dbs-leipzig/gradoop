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

public class DropVerticesWithInfrequentLabels extends
  RichMapFunction<GraphTransaction, GraphTransaction> {

  private Collection<String> frequentVertexLabels;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    this.frequentVertexLabels = getRuntimeContext()
      .getBroadcastVariable(Constants.VERTEX_DICTIONARY);

    this.frequentVertexLabels = Sets.newHashSet(frequentVertexLabels);
  }


  @Override
  public GraphTransaction map(GraphTransaction value) throws Exception {

    Set<GradoopId> keptVertexIds = Sets.newHashSet();

    Iterator<Vertex> vertexIterator = value.getVertices().iterator();

    while (vertexIterator.hasNext()) {
      Vertex vertex = vertexIterator.next();

      if (frequentVertexLabels.contains(vertex.getLabel())) {
        keptVertexIds.add(vertex.getId());
      } else {
        vertexIterator.remove();
      }
    }

    Iterator<Edge> edgeIterator = value.getEdges().iterator();

    while (edgeIterator.hasNext()) {
      Edge edge = edgeIterator.next();

      if (! (keptVertexIds.contains(edge.getSourceId()) &&
        keptVertexIds.contains(edge.getTargetId()))) {
        edgeIterator.remove();
      }
    }

    return value;
  }
}
