
package org.gradoop.flink.datagen.transactions.foodbroker.functions.masterdata;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.datagen.transactions.foodbroker.config.Constants;
import org.gradoop.flink.datagen.transactions.foodbroker.tuples.EmployeeData;

/**
 * Creates a tuple from the given vertex. The tuple consists of the gradoop id and the relevant
 * person data.
 */
public class EmployeeDataMapper implements MapFunction<Vertex, Tuple2<GradoopId, EmployeeData>> {

  /**
   * Reduce object instantiation.
   */
  private EmployeeData reuseEmployeeData;

  /**
   * Constructor for object instantiation.
   */
  public EmployeeDataMapper() {
    reuseEmployeeData = new EmployeeData();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Tuple2<GradoopId, EmployeeData> map(Vertex v) throws Exception {
    reuseEmployeeData.setQuality(v.getPropertyValue(Constants.QUALITY_KEY).getFloat());
    reuseEmployeeData.setCity(v.getPropertyValue(Constants.CITY_KEY).getString());
    return new Tuple2<>(v.getId(), reuseEmployeeData);
  }
}
