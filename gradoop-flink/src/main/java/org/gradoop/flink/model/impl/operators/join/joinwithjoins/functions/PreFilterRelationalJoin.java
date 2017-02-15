package org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.join.JoinType;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.utils.JoinWithJoinsUtils;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.utils.OptSerializable;
import org.gradoop.flink.model.impl.operators.join.joinwithjoins.utils.OptSerializableGradoopId;

import java.io.Serializable;

/**
 * Given a set of vertex elements that have to be pre-filtered accordingly to the vertex join
 * semantics, performs a pre-filtering and pre-processing step by selecting only those vertices
 * that really will be involved in the final result.
 *
 * Created by Giacomo Bergami on 14/02/17.
 */
public class PreFilterRelationalJoin implements PreFilter<Vertex, GradoopId>, Serializable {

  /**
   * Checking if the vertex dataset comes from the left operand or not
   */
  private final boolean isLeft;


  private volatile DataSet<Edge> e;
  private final JoinType vertexJoinType;

  public PreFilterRelationalJoin(Boolean isLeft, DataSet<Edge> e, JoinType vertexJoinType) {
    this.isLeft = isLeft;
    this.e = e;
    this.vertexJoinType = vertexJoinType;
  }

  @Override
  public DataSet<Tuple2<Vertex,OptSerializableGradoopId>> apply(DataSet<Vertex> vertexDataSet) {
    return  JoinWithJoinsUtils.joinByVertexEdge(vertexDataSet,e,vertexJoinType,isLeft)
      .where(new Id<>())
      .equalTo(new KeySelectorFromVertexSrcOrDest(isLeft))
      .with(new JoinFunctionWithVertexAndGradoopIdFromEdge());
  }
}
