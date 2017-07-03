
package org.gradoop.flink.algorithms.fsm.transactional.tle.tuples;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.model.api.tuples.Countable;

/**
 * (category, label, frequency)
 */
public class CategoryCountableLabel extends Tuple3<String, String, Long>
  implements Categorizable, Countable {

  /**
   * Default constructor.
   */
  public CategoryCountableLabel() {
  }

  /**
   * Constructor.
   *
   * @param category category
   * @param label label
   * @param count count
   */
  public CategoryCountableLabel(String category, String label, Long count) {
    super(category, label, count);
  }

  @Override
  public String getCategory() {
    return f0;
  }

  @Override
  public void setCategory(String category) {
    f0 = category;
  }

  public String getLabel() {
    return f1;
  }

  public void setLabel(String label) {
    f1 = label;
  }

  @Override
  public long getCount() {
    return f2;
  }

  @Override
  public void setCount(long count) {
    f2 = count;
  }
}
