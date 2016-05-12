package org.gradoop.model.impl.datagen.foodbroker.generators;

import org.gradoop.model.impl.algorithms.btgs.BusinessTransactionGraphs;
import org.gradoop.model.impl.datagen.foodbroker.Constants;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataSeed;
import org.gradoop.model.impl.properties.PropertyList;

public class MasterData {
  static String createBusinessIdentifier(MasterDataSeed seed, String acronym) {

    String idString = seed.getNumber().toString();

    for(int i = 1; i <= (8 - idString.length()); i++) {
      idString = "0" + idString;
    }

    return acronym + idString;
  }

  public static PropertyList createDefaultProperties(String acronym,
    MasterDataSeed seed) {

    String bid = createBusinessIdentifier(seed, acronym);

    PropertyList properties = new PropertyList();

    properties.set("num", bid);
    properties.set(Constants.QUALITY, seed.getQuality());
    properties.set(BusinessTransactionGraphs.SUPERTYPE_KEY,
      BusinessTransactionGraphs.SUPERCLASS_VALUE_MASTER);
    properties.set(BusinessTransactionGraphs.SOURCEID_KEY, "ERP_" + bid);

    return properties;
  }
}
