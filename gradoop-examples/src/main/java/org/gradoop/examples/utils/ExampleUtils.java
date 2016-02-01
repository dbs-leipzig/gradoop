package org.gradoop.examples.utils;

import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.cam.CanonicalAdjacencyMatrix;
import org.gradoop.model.impl.operators.cam.functions.EdgeDataLabeler;
import org.gradoop.model.impl.operators.cam.functions.GraphHeadDataLabeler;
import org.gradoop.model.impl.operators.cam.functions.VertexDataLabeler;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;

public class ExampleUtils {
  public static void print(
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> graph) throws Exception {

    print(GraphCollection.fromGraph(graph));

  }

  public static void print(
    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> collection) throws
    Exception {

    new CanonicalAdjacencyMatrix<>(
      new GraphHeadDataLabeler<GraphHeadPojo>(),
      new VertexDataLabeler<VertexPojo>(),
      new EdgeDataLabeler<EdgePojo>()
    ).execute(collection).print();

  }
}
