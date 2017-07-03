
package org.gradoop.flink.model.impl.operators.aggregation.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateDefaultValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Sets an aggregate value of a graph head.
 */
@FunctionAnnotation.ForwardedFields("id")
public class SetAggregateProperty
  extends RichMapFunction<GraphHead, GraphHead> {

  /**
   * constant string for accessing broadcast variable "property value"
   */
  public static final String VALUE = "value";

  /**
   * aggregate property key
   */
  private final String propertyKey;

  /**
   * aggregate value
   */
  private PropertyValue aggregateValue;

  /**
   * default value used to replace aggregate value in case of NULL.
   */
  private final PropertyValue defaultValue;


  /**
   * Constructor.
   *
   * @param aggregateFunction aggregate function
   */
  public SetAggregateProperty(AggregateFunction aggregateFunction) {
    checkNotNull(aggregateFunction);

    this.propertyKey = aggregateFunction.getAggregatePropertyKey();

    this.defaultValue = aggregateFunction instanceof AggregateDefaultValue ?
      ((AggregateDefaultValue) aggregateFunction).getDefaultValue() :
      PropertyValue.NULL_VALUE;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.aggregateValue =
      (PropertyValue) getRuntimeContext().getBroadcastVariable(VALUE).get(0);

    if (aggregateValue.equals(PropertyValue.NULL_VALUE)) {
      aggregateValue = defaultValue;
    }
  }

  @Override
  public GraphHead map(GraphHead graphHead) throws Exception {
    graphHead.setProperty(propertyKey, aggregateValue);
    return graphHead;
  }
}
