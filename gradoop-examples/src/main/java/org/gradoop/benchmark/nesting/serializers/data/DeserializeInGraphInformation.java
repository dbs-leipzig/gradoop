package org.gradoop.benchmark.nesting.serializers.data;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.GraphElement;

import java.util.Iterator;

/**
 * Serializing a vertex in a proper way
 */
public class DeserializeInGraphInformation
  implements MapFunction<String, Tuple2<GradoopId, GradoopIdList>> {


  /**
   * Reusable builder
   */
  private Tuple2<GradoopId, GradoopIdList> sb;

  /**
   * Default constructor
   */
  public DeserializeInGraphInformation() {
    sb = new Tuple2<>();
    sb.f1 = new GradoopIdList();
  }

  @Override
  public Tuple2<GradoopId, GradoopIdList> map(String value) throws Exception {
    sb.f1.clear();
    String[] array = value.split(",");
    sb.f0 = GradoopId.fromString(array[0]);
    for (int i = 1; i<array.length; i++) {
      sb.f1.add(GradoopId.fromString(array[i]));
    }
    return sb;
  }
}
