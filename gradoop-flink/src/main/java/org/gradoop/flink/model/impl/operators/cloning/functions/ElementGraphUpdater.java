
package org.gradoop.flink.model.impl.operators.cloning.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;

/**
 * Replaces the graph set of each element by a new one, containing only the
 * first element of a broadcast GradoopId DataSet.
 *
 * @param <EL> EPGM element type
 */
@FunctionAnnotation.ForwardedFields("id;label;properties")
public class ElementGraphUpdater<EL extends GraphElement>
  extends RichMapFunction<EL, EL> {

  /**
   * constant string for accessing broadcast variable "graph id"
   */
  public static final String GRAPHID = "graph id";

  /**
   * id of the new graph
   */
  private GradoopId graphId;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.graphId = (GradoopId) getRuntimeContext()
      .getBroadcastVariable(GRAPHID).get(0);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EL map(EL element) {
    element.setGraphIds(GradoopIdList.fromExisting(graphId));
    return element;
  }
}
