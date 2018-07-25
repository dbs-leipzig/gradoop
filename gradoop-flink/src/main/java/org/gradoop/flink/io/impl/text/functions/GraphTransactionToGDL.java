package org.gradoop.flink.io.impl.text.functions;

import akka.stream.Graph;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.text.GDLEncoder;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

import java.util.ArrayList;
import java.util.List;

public class GraphTransactionToGDL implements MapFunction<GraphTransaction, String> {
  @Override
  public String map(GraphTransaction graphTransaction) throws Exception {
    List<Vertex> vertices = new ArrayList<>();
    vertices.addAll(graphTransaction.getVertices());

    List<Edge> edges = new ArrayList<>();
    edges.addAll(graphTransaction.getEdges());

    GraphHead graphHead = graphTransaction.getGraphHead();

    GDLEncoder encoder = new GDLEncoder(graphHead, vertices, edges);
    return encoder.graphToGDLString();
  }
}
