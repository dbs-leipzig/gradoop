package org.gradoop.flink.datagen.foodbroker.functions;


import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Created by Stephan on 01.08.16.
 */
public class TransactionalVerticesFromVertices
  implements FilterFunction<Vertex> {

  @Override
  public boolean filter(Vertex v) throws Exception {
    if (v.hasProperty("kind")) {
      if (v.getPropertyValue("kind").toString().equals("TransData")) {
        return true;
      }
    }
    return false;
  }
}
