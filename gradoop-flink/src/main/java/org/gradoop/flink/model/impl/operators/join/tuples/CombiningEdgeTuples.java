package org.gradoop.flink.model.impl.operators.join.tuples;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.io.Serializable;

/**
 * Created by Giacomo Bergami on 30/01/17.
 */
public class CombiningEdgeTuples extends Tuple3<Vertex,Edge,Vertex> implements Serializable {
  public CombiningEdgeTuples(Vertex f0, Edge f1, Vertex f11) {
    super(f0,f1,f11);
  }
  public CombiningEdgeTuples() {}
}
