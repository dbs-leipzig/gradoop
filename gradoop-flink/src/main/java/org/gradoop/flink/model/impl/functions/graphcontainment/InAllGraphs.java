
package org.gradoop.flink.model.impl.functions.graphcontainment;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.id.GradoopIdList;

/**
 * True, if an element is contained in all of a set of given graphs.
 *
 * @param <GE> element type
 */
@FunctionAnnotation.ReadFields("graphIds")
public class InAllGraphs<GE extends GraphElement>
  implements FilterFunction<GE> {

  /**
   * graph ids
   */
  private final GradoopIdList graphIds;

  /**
   * constructor
   *
   * @param graphIds graph ids
   */
  public InAllGraphs(GradoopIdList graphIds) {
    this.graphIds = graphIds;
  }

  @Override
  public boolean filter(GE element) throws Exception {
    return element.getGraphIds().containsAll(this.graphIds);
  }
}
