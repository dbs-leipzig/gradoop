
package org.gradoop.flink.model.impl.functions.graphcontainment;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * True, if an element is contained in a give graph.
 *
 * @param <EL> element type
 */
@FunctionAnnotation.ReadFields("graphIds")
public class InGraph<EL extends GraphElement>
  implements FilterFunction<EL> {

  /**
   * graph id
   */
  private final GradoopId graphId;

  /**
   * constructor
   * @param graphId graph id
   */
  public InGraph(GradoopId graphId) {
    this.graphId = graphId;
  }

  @Override
  public boolean filter(EL element) throws Exception {
    return element.getGraphIds().contains(this.graphId);
  }
}
