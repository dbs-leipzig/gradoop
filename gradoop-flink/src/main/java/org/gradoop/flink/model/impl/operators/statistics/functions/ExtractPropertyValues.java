
package org.gradoop.flink.model.impl.operators.statistics.functions;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.Set;

/**
 * Extracts Triples of the form <Tuple1<PropertyName>, PropertyValue> from the given list of
 * GraphElements
 * @param <T> graph element type
 */
public class ExtractPropertyValues<T extends GraphElement>
  implements FlatMapFunction<T, Tuple2<String, Set<PropertyValue>>> {

  /**
   * Reuse Tuple
   */
  private final Tuple2<String, Set<PropertyValue>> reuseTuple;

  /**
   * Creates a new UDF
   */
  public ExtractPropertyValues() {
    this.reuseTuple = new Tuple2<>();
  }

  @Override
  public void flatMap(T value, Collector<Tuple2<String, Set<PropertyValue>>> out) throws Exception {
    for (Property property : value.getProperties()) {
      reuseTuple.f0 = property.getKey();
      reuseTuple.f1 = Sets.newHashSet(property.getValue());

      out.collect(reuseTuple);
    }
  }
}
