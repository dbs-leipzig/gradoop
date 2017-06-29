package org.gradoop.flink.model.impl.operators.drilling;

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.drilling.functions.transformations.RollUpTransformation;
import org.gradoop.flink.model.impl.operators.drilling.functions.drillfunctions.DrillFunction;

/**
 * Roll up operation TODO complete with example.
 */
public class RollUp extends Drill {

  /**
   * Valued constructor.
   *
   * @param label          label of the element whose property shall be drilled, or
   *                       see {@link Drill#DRILL_ALL_ELEMENTS}
   * @param propertyKey    property key
   * @param function       drill function which shall be applied to a property
   * @param newPropertyKey new property key, or see {@link Drill#KEEP_CURRENT_PROPERTY_KEY}
   * @param drillVertex    true, if vertices shall be drilled, false for edges
   */
  public RollUp(
    String label, String propertyKey, DrillFunction function, String newPropertyKey,
    boolean drillVertex) {
    super(label, propertyKey, function, newPropertyKey, drillVertex);
  }


  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    if (isDrillVertex()) {
      graph = graph.transformVertices(
        new RollUpTransformation<Vertex>(getLabel(), getPropertyKey(), getFunction(),
          getNewPropertyKey()));
    } else {
      graph = graph.transformEdges(
        new RollUpTransformation<Edge>(getLabel(), getPropertyKey(), getFunction(),
          getNewPropertyKey()));
    }
    return graph;
  }

  @Override
  public String getName() {
    return RollUp.class.getName();
  }

}
