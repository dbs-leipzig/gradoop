
package org.gradoop.flink.model.impl.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;

/**
 * (id, {id, id, ...})
 */
public class IdWithIdSet extends Tuple2<GradoopId, GradoopIdList> {

  public GradoopId getId() {
    return f0;
  }

  public void setId(GradoopId id) {
    f0 = id;
  }

  public GradoopIdList getIdSet() {
    return f1;
  }

  public void setIdSet(GradoopIdList idSet) {
    f1 = idSet;
  }
}
