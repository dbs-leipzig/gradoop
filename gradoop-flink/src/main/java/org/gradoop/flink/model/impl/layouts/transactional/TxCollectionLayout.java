package org.gradoop.flink.model.impl.layouts.transactional;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayout;
import org.gradoop.flink.representation.transactional.GraphTransaction;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class TxCollectionLayout implements GraphCollectionLayout {

  private final DataSet<GraphTransaction> transactions;

  TxCollectionLayout(DataSet<GraphTransaction> transactions) {
    this.transactions = transactions;
  }

  @Override
  public DataSet<GraphHead> getGraphHeads() {
    throw new NotImplementedException();
  }

  @Override
  public DataSet<GraphHead> getGraphHeadsByLabel(String label) {
    throw new NotImplementedException();
  }

  @Override
  public DataSet<GraphTransaction> getGraphTransactions() {
    return transactions;
  }

  @Override
  public DataSet<Vertex> getVertices() {
    throw new NotImplementedException();
  }

  @Override
  public DataSet<Vertex> getVerticesByLabel(String label) {
    throw new NotImplementedException();
  }

  @Override
  public DataSet<Edge> getEdges() {
    throw new NotImplementedException();
  }

  @Override
  public DataSet<Edge> getEdgesByLabel(String label) {
    throw new NotImplementedException();
  }

  @Override
  public DataSet<Edge> getOutgoingEdges(GradoopId vertexID) {
    throw new NotImplementedException();
  }

  @Override
  public DataSet<Edge> getIncomingEdges(GradoopId vertexID) {
    throw new NotImplementedException();
  }
}
