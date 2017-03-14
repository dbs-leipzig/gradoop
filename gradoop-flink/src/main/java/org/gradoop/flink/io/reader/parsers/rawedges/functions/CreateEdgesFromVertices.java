package org.gradoop.flink.io.reader.parsers.rawedges.functions;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;

import java.util.ArrayList;

/**
 * Defines the edges from the sole vertices
 */
public class CreateEdgesFromVertices implements GroupCombineFunction<Tuple2<GradoopId,GradoopId>,
  Tuple2<GradoopId,Edge>> {

  /**
   * Array of vertices. This list is updated when a new input is read
   */
  private ArrayList<GradoopId> elements;

  /**
   * I want to create new edges. So, I want to create new ids
   */
  private EdgeFactory ef;

  /**
   * Reusable element to be returned
   */
  private Tuple2<GradoopId, Edge> reusable;

  /**
   * For defining the graph, I have to specify to which graph the edge belongs
   */
  private GradoopIdList gil;

  public CreateEdgesFromVertices(EdgeFactory ef) {
    this.ef = ef;
    this.elements = new ArrayList<>();
    this.reusable = new Tuple2<>();
    gil = new GradoopIdList();
  }

  @Override
  public void combine(Iterable<Tuple2<GradoopId, GradoopId>> values,
    Collector<Tuple2<GradoopId, Edge>> out) throws Exception {
    int listSize = elements.size();
    gil.clear();
    int count = 0;
    for (Tuple2<GradoopId,GradoopId> x : values) {
      reusable.f0 = x.f0; // Element over which the combination is defined
      if (listSize>count) {
        elements.set(count++,x.f1);
      } else {
        elements.add(x.f1);
      }
    }
    gil.add(reusable.f0);
    for (int i=0; i<count; i++) {
      GradoopId src = elements.get(i);
      for (int j=0; j<count; j++) {
        GradoopId dst = elements.get(j);
        reusable.f1 = ef.createEdge("Edge"+reusable.f0.toString(),src,dst,gil);
        out.collect(reusable);
      }
    }
  }

}
