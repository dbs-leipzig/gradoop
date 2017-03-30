package org.gradoop.flink.model.impl.operators.nest.transformations;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.operators.nest.functions.keys.SelfId;
import org.gradoop.flink.model.impl.operators.nest.functions.UpdateEdges;
import org.gradoop.flink.model.impl.operators.nest.functions.UpdateVertices;
import org.gradoop.flink.model.impl.operators.nest.functions.VertexToGraphHead;
import org.gradoop.flink.model.impl.operators.nest.functions.map.MapGraphHeadAsVertex;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestedIndexing;
import org.gradoop.flink.model.impl.operators.nest.model.NormalizedGraph;

/**
 * Created by vasistas on 29/03/17.
 */
public class NestedIndexingToEPGMTransformations {

  public static GraphCollection toGraphCollection(NestedIndexing self,
    NormalizedGraph dataLake) {

    DataSet<Vertex> vertices = self.getGraphHeadToVertex()
      .coGroup(dataLake.getVertices())
      .where(new Value1Of2<>()).equalTo(new Id<>())
      .with(new UpdateVertices());

    DataSet<Edge> edges = self.getGraphHeadToEdge()
      .coGroup(dataLake.getEdges())
      .where(new Value1Of2<>()).equalTo(new Id<>())
      .with(new UpdateEdges());

    DataSet<GraphHead> heads = getActualGraphHeads(self,dataLake);

    return GraphCollection.fromDataSets(heads, vertices, edges, dataLake.getConfig());
  }

  public static LogicalGraph toLogicalGraph(NestedIndexing self,
    NormalizedGraph dataLake) {

    DataSet<Vertex> vertices = self.getGraphHeadToVertex()
      .coGroup(dataLake.getVertices())
      .where(new Value1Of2<>()).equalTo(new Id<>())
      .with(new UpdateVertices());

    DataSet<Edge> edges = self.getGraphHeadToEdge()
      .coGroup(dataLake.getEdges())
      .where(new Value1Of2<>()).equalTo(new Id<>())
      .with(new UpdateEdges());

    DataSet<GraphHead> heads = getActualGraphHeads(self,dataLake);

    return LogicalGraph.fromDataSets(heads, vertices, edges, dataLake.getConfig());
  }

  /**
   * Instantiates the GraphHeads using the LogicalGraph as a primary source
   * @param self      ground truth for elements
   * @param dataLake  primary source
   * @return          GraphHeads
   */
  public DataSet<GraphHead> getActualGraphHeads(NestedIndexing self, LogicalGraph dataLake) {
    return self.getGraphHeads()
      .join(dataLake.getVertices())
      .where(new SelfId()).equalTo(new Id<>())
      .with(new VertexToGraphHead());
  }

  /**
   * Instantiates the GraphHeads using the LogicalGraph as a primary source
   * @param self      ground truth for elements
   * @param dataLake  primary source
   * @return          GraphHeads
   */
  public static DataSet<GraphHead> getActualGraphHeads(NestedIndexing self,
                                                       NormalizedGraph dataLake) {
    return self.getGraphHeads()
      .join(dataLake.getVertices())
      .where(new SelfId()).equalTo(new Id<>())
      .with(new VertexToGraphHead());
  }

  /**
   * Recreates the NormalizedGraph of the informations stored within
   * the NestedIndexing through the DataLake
   * @param ni        Element to be transformed
   * @param dataLake  Element containing all the required and necessary
   *                  informations
   * @return          Actual values
   */
  public static NormalizedGraph asNormalizedGraph(NestedIndexing ni, NormalizedGraph dataLake) {
    DataSet<Vertex> vertices = ni.getGraphHeadToVertex()
      .coGroup(dataLake.getVertices())
      .where(new Value1Of2<>()).equalTo(new Id<>())
      .with(new UpdateVertices());

    DataSet<Edge> edges = ni.getGraphHeadToEdge()
      .coGroup(dataLake.getEdges())
      .where(new Value1Of2<>()).equalTo(new Id<>())
      .with(new UpdateEdges());

    DataSet<GraphHead> heads = getActualGraphHeads(ni,dataLake);

    vertices = vertices
      .union(heads.map(new MapGraphHeadAsVertex()))
      .distinct(new Id<>());

    return new NormalizedGraph(heads, vertices, edges, dataLake.getConfig());
  }

}
