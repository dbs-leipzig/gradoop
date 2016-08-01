package org.gradoop.model.impl.datagen.foodbroker.tuples;

import org.gradoop.model.impl.id.GradoopId;

/**
 * Created by Stephan on 25.07.16.
 */
public class MasterDataTuple //extends Tuple2<GradoopId, Float>
//  implements MDTuple {
extends AbstractMasterDataTuple {

  public MasterDataTuple() {
  }

  public MasterDataTuple(GradoopId id, Float quality) {
    super(id, quality);
  }
}
