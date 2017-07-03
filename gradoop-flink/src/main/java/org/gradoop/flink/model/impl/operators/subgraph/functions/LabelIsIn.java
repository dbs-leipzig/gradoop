
package org.gradoop.flink.model.impl.operators.subgraph.functions;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.Element;

import java.util.Collection;

/**
 * Filter function to check if an EPGM element's label is in a white list.
 *
 * @param <EL> element type
 */
public class LabelIsIn<EL extends Element> implements FilterFunction<EL> {

  /**
   * White list of labels.
   */
  private final Collection<String> labels;

  /**
   * Constructor.
   * @param labels white list of labels
   */
  public LabelIsIn(String... labels) {
    this.labels = Sets.newHashSet(labels);
  }

  @Override
  public boolean filter(EL element) throws Exception {
    return labels.contains(element.getLabel());
  }
}
