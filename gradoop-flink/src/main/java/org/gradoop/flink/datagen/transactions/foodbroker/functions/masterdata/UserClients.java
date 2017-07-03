
package org.gradoop.flink.datagen.transactions.foodbroker.functions.masterdata;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.Set;

/**
 * Collects all users and clients given by sets into one dataset.
 */
public class UserClients implements FlatMapFunction<Set<Vertex>, Vertex> {

  @Override
  public void flatMap(Set<Vertex> vertices,
    Collector<Vertex> collector) throws Exception {
    for (Vertex vertex : vertices) {
      collector.collect(vertex);
    }
  }
}
