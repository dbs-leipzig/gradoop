package org.gradoop.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.NullValue;
import org.gradoop.model.api.EPGMElement;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.properties.PropertyValue;

/**
 * element -> (elementId, propertyValue)
 *
 * @param <EL> EPGM element type
 */
@FunctionAnnotation.ForwardedFields("id->f0")
public class PairElementWithPropertyValue<EL extends EPGMElement>
  implements MapFunction<EL, Tuple2<GradoopId, PropertyValue>> {

  /**
   * Used to access property value to return.
   */
  private final String propertyKey;

  /**
   * Reduce instantiations.
   */
  private final Tuple2<GradoopId, PropertyValue> reuseTuple;

  /**
   * Constructor.
   *
   * @param propertyKey used to access property value
   */
  public PairElementWithPropertyValue(String propertyKey) {
    this.propertyKey  = propertyKey;
    this.reuseTuple   = new Tuple2<>();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Tuple2<GradoopId, PropertyValue> map(EL el) throws Exception {
    reuseTuple.f0 = el.getId();
    if (el.hasProperty(propertyKey)) {
      reuseTuple.f1 = el.getPropertyValue(propertyKey);
    } else {
      reuseTuple.f1 = PropertyValue.NULL_VALUE;
    }
    return reuseTuple;
  }
}
