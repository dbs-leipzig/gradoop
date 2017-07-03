
package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.flink.io.impl.csv.metadata.MetaDataParser;

import java.util.HashSet;
import java.util.Set;

/**
 * (element) -> (elementLabel, {key_1:type_1,key_2:type_2,...,key_n:type_n})
 *
 * @param <E> EPGM element type
 */
@FunctionAnnotation.ForwardedFields("label->f0")
public class ElementToPropertyMetaData<E extends Element> implements MapFunction<E, Tuple2<String, Set<String>>> {
  /**
   * Reduce object instantiations.
   */
  private final Tuple2<String, Set<String>> reuseTuple;
  /**
   * Constructor
   */
  public ElementToPropertyMetaData() {
    reuseTuple = new Tuple2<>();
    reuseTuple.f1 = new HashSet<>();
  }

  @Override
  public Tuple2<String, Set<String>> map(E e) throws Exception {
    reuseTuple.f0 = e.getLabel();
    reuseTuple.f1.clear();
    for (Property property : e.getProperties()) {
      reuseTuple.f1.add(MetaDataParser.getPropertyMetaData(property));
    }
    return reuseTuple;
  }
}
