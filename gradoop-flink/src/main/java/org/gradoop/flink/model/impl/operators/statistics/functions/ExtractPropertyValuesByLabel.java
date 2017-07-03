
package org.gradoop.flink.model.impl.operators.statistics.functions;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.Set;

/**
 * Extracts Triples of the form <Tuple2<Label,PropertyName>, PropertyValue> from the given list of
 * GraphElements
 * @param <T> graph element type
 */
@FunctionAnnotation.ForwardedFields("label->0.0")
public class ExtractPropertyValuesByLabel<T extends GraphElement>
  extends RichFlatMapFunction<T, Tuple2<Tuple2<String, String>, Set<PropertyValue>>> {

  /**
   * Reuse Tuple
   */
  private final Tuple2<Tuple2<String, String>, Set<PropertyValue>> reuseTuple;

  /**
   * Creates a new UDF
   */
  public ExtractPropertyValuesByLabel() {
    this.reuseTuple = new Tuple2<>();
    this.reuseTuple.f0 = new Tuple2<>();
  }

  @Override
  public void flatMap(T value, Collector<Tuple2<Tuple2<String, String>, Set<PropertyValue>>> out)
      throws Exception {

    for (Property property : value.getProperties()) {
      reuseTuple.f0.f0 = value.getLabel();
      reuseTuple.f0.f1 = property.getKey();
      reuseTuple.f1 = Sets.newHashSet(property.getValue());

      out.collect(reuseTuple);
    }
  }
}
