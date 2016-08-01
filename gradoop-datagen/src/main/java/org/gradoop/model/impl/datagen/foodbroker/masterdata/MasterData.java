package org.gradoop.model.impl.datagen.foodbroker.masterdata;

import org.gradoop.model.impl.datagen.foodbroker.config.Constants;
import org.gradoop.model.impl.datagen.foodbroker.tuples.MasterDataSeed;
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
    properties.set(Constants.SUPERTYPE_KEY,
      Constants.SUPERCLASS_VALUE_MASTER);
    properties.set(Constants.SOURCEID_KEY, "ERP_" + bid);

    return properties;
  }
}
