
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.id.GradoopIdList;

import java.util.Iterator;

/**
 * Creates a single graph element which is contained in all graphs that the
 * input elements are contained in.
 *
 * GraphElement* -> GraphElement
 *
 * @param <GE> EPGM graph element type
 */
public class MergedGraphIds<GE extends GraphElement>
  implements GroupCombineFunction<GE, GE>, GroupReduceFunction<GE, GE>,
  JoinFunction<GE, GE, GE> {


  @Override
  public void combine(Iterable<GE> values, Collector<GE> out) throws Exception {
    reduce(values, out);
  }

  @Override
  public void reduce(Iterable<GE> values, Collector<GE> out) throws Exception {
    Iterator<GE> iterator = values.iterator();
    GE result = iterator.next();
    GradoopIdList graphIds = result.getGraphIds();
    while (iterator.hasNext()) {
      graphIds.addAll(iterator.next().getGraphIds());
    }
    out.collect(result);
  }

  @Override
  public GE join(GE first, GE second) throws Exception {
    first.getGraphIds().addAll(second.getGraphIds());
    return first;
  }
}
