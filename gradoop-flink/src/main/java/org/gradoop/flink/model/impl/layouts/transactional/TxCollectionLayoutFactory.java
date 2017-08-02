package org.gradoop.flink.model.impl.layouts.transactional;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayout;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayoutFactory;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayout;
import org.gradoop.flink.representation.transactional.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Collection;

public class TxCollectionLayoutFactory implements GraphCollectionLayoutFactory {
  @Override
  public GraphCollectionLayout fromDataSets(DataSet<GraphHead> graphHeads,
    DataSet<Vertex> vertices, GradoopFlinkConfig config) {
    throw new NotImplementedException();
  }

  @Override
  public GraphCollectionLayout fromDataSets(DataSet<GraphHead> graphHeads, DataSet<Vertex> vertices,
    DataSet<Edge> edges, GradoopFlinkConfig config) {
    throw new NotImplementedException();
  }

  @Override
  public GraphCollectionLayout fromCollections(Collection<GraphHead> graphHeads,
    Collection<Vertex> vertices, Collection<Edge> edges, GradoopFlinkConfig config) {
    throw new NotImplementedException();
  }

  @Override
  public GraphCollectionLayout fromGraphLayout(LogicalGraphLayout logicalGraphLayout,
    GradoopFlinkConfig config) {
    throw new NotImplementedException();
  }

  @Override
  public GraphCollectionLayout fromTransactions(DataSet<GraphTransaction> transactions,
    GradoopFlinkConfig config) {
    return new TxCollectionLayout(transactions);
  }

  @Override
  public GraphCollectionLayout fromTransactions(DataSet<GraphTransaction> transactions,
    GroupReduceFunction<Vertex, Vertex> vertexMergeReducer,
    GroupReduceFunction<Edge, Edge> edgeMergeReducer,
    GradoopFlinkConfig config) {
    throw new NotImplementedException();
  }

  @Override
  public GraphCollectionLayout createEmptyCollection(GradoopFlinkConfig config) {
    throw new NotImplementedException();
  }
}
