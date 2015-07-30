package org.gradoop.model.impl.operators;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.impl.EPFlinkVertexData;

import java.io.Serializable;

/**
 * Created by niklas on 29.07.15.
 */
public interface LongFromVertexFunction extends Serializable, Function {

  Long extractLong(Vertex<Long, EPFlinkVertexData> vertex);
}
