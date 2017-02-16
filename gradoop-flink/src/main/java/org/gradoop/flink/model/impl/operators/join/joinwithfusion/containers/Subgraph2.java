package org.gradoop.flink.model.impl.operators.join.joinwithfusion.containers;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Created by vasistas on 16/02/17.
 */
public class Subgraph2 extends DataSet<Vertex> {

  protected Subgraph2(ExecutionEnvironment context, TypeInformation<Vertex> typeInfo) {
    super(context, typeInfo);
  }


}
