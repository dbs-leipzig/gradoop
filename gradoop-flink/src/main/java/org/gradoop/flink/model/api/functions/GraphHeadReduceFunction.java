
package org.gradoop.flink.model.api.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.GraphHead;

import java.io.Serializable;

/**
 * Marker interface for reduce functions used for group by isomorphism operator.
 * Such functions can be used to calculate aggregates based on graph heads.
 * For example, to count isomorphic graphs in a collection.
 */
public interface GraphHeadReduceFunction
  extends GroupReduceFunction<Tuple2<String, GraphHead>, GraphHead>, Serializable {
}
