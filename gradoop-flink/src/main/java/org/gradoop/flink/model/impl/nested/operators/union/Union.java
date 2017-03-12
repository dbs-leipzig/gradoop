package org.gradoop.flink.model.impl.nested.operators.union;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.nested.IdGraphDatabase;
import org.gradoop.flink.model.impl.nested.operators.UnaryOp;
import org.gradoop.flink.model.impl.nested.datastructures.DataLake;
import org.gradoop.flink.model.impl.nested.operators.union.functions.SubsituteHead;

/**
 * Implements the union for the nesting model
 */
public class Union extends UnaryOp {

  /**
   * Graph id for the final resulting graph
   */
  private final GradoopId id;

  /**
   * Use a specific graph id for the graph that has to be returned
   * @param id
   */
  public Union(GradoopId id) {
    this.id = id;
  }

  /**
   * Use a random graph id for the final union graph
   */
  public Union() {
    this(GradoopId.get());
  }

  @Override
  protected IdGraphDatabase runWithArgAndLake(DataLake lake, IdGraphDatabase data) {
    DataSet<GradoopId> head = lake.asNormalizedGraph().getConfig().getExecutionEnvironment()
      .fromElements(id);
    DataSet<Tuple2<GradoopId, GradoopId>> vertices = data.getGraphHeadToVertex()
      .flatMap(new SubsituteHead(id))
      .distinct(1);
    DataSet<Tuple2<GradoopId, GradoopId>> edges = data.getGraphHeadToEdge()
      .flatMap(new SubsituteHead(id))
      .distinct(1);
    return new IdGraphDatabase(head, vertices, edges);
  }

  @Override
  public String getName() {
    return Union.class.getName();
  }

}
