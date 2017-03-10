package org.gradoop.flink.model.impl.nested.algorithms.union;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.nested.IdGraphDatabase;
import org.gradoop.flink.model.impl.nested.algorithms.UnaryOp;
import org.gradoop.flink.model.impl.nested.datastructures.DataLake;

/**
 * Created by vasistas on 10/03/17.
 */
public class Union extends UnaryOp {

  private final GradoopId id;

  public Union(GradoopId id) {
    this.id = id;
  }

  public Union() {
    this(GradoopId.get());
  }

  @Override
  protected IdGraphDatabase runWithArgAndLake(DataLake lake, IdGraphDatabase data) {
    DataSet<GradoopId> head = lake.asNormalizedGraph().getConfig().getExecutionEnvironment()
      .fromElements(id);
    DataSet<Tuple2<GradoopId, GradoopId>> vertices = data.getGraphHeadToVertex()
      .map(new SubsituteHead(id))
      .distinct(1);
    DataSet<Tuple2<GradoopId, GradoopId>> edges = data.getGraphHeadToEdge()
      .map(new SubsituteHead(id))
      .distinct(1);
    return new IdGraphDatabase(head,vertices,edges);
  }

  @Override
  public String getName() {
    return Union.class.getName();
  }

}
