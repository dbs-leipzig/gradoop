package org.gradoop.flink.model.impl.operators.join.functions;

import com.fasterxml.jackson.databind.type.TypeParser;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.join.JoinType;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.join.JoinUtils;
import org.gradoop.flink.model.impl.operators.join.blocks.KeySelectorFromVertexSrcOrDest;
import org.gradoop.flink.model.impl.operators.join.operators.OptSerializable;
import org.gradoop.flink.model.impl.operators.join.operators.OptSerializableGradoopId;
import org.gradoop.flink.model.impl.operators.join.operators.PreFilter;

import java.io.Serializable;

/**
 * Created by Giacomo Bergami on 14/02/17.
 */
public class RelationalJoinPrefilter implements PreFilter<Vertex, GradoopId>, Serializable {
  private final boolean isLeft;
  private volatile DataSet<Edge> e;
  private final JoinType vertexJoinType;

  public RelationalJoinPrefilter(Boolean isLeft, DataSet<Edge> e, JoinType vertexJoinType) {
    this.isLeft = isLeft;
    this.e = e;
    this.vertexJoinType = vertexJoinType;
  }

  @Override
  public DataSet<Tuple2<Vertex,OptSerializableGradoopId>> apply(DataSet<Vertex> vertexDataSet) {
    System.out.println(OptSerializable.class.getName());
    return  JoinUtils.joinByVertexEdge(vertexDataSet,e,vertexJoinType,isLeft)
      .where((Vertex v) -> v.getId())
      .equalTo(new KeySelectorFromVertexSrcOrDest(isLeft))
      .with(new JoinFunction<Vertex, Edge, Tuple2<Vertex,OptSerializableGradoopId>>() {
        @Override
        public Tuple2<Vertex,OptSerializableGradoopId> join(Vertex first, Edge second) throws
          Exception {
          return new Tuple2<>(first, second == null ? OptSerializableGradoopId.empty() :
            OptSerializableGradoopId.value(second.getId()));
        }
      })
      .returns(TypeInfoParser.parse(Tuple2.class.getCanonicalName()+"<"+Vertex.class
        .getCanonicalName()+","+OptSerializableGradoopId.class.getName()+">"));
  }
}
