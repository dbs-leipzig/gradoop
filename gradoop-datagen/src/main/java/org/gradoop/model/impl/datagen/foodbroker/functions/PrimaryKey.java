package org.gradoop.model.impl.datagen.foodbroker.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataObject;

/**
 * Created by peet on 24.03.16.
 */
public class PrimaryKey implements KeySelector<MasterDataObject, Long> {

  @Override
  public Long getKey(MasterDataObject masterDataObject) throws Exception {
    return masterDataObject.getPrimaryKey();
  }
}
