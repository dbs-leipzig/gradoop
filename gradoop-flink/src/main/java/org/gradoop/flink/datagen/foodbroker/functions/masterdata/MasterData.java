package org.gradoop.flink.datagen.foodbroker.functions.masterdata;


import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.flink.datagen.foodbroker.config.Constants;
import org.gradoop.flink.datagen.foodbroker.tuples.MasterDataSeed;

public class MasterData {
  private static String createBusinessIdentifier(MasterDataSeed seed, String
    acronym) {

    String idString = String.valueOf(seed.getNumber());
    long count = 8 - idString.length();

    for(int i = 1; i <= (count); i++) {
      idString = "0" + idString;
    }

    return acronym + idString;
  }

  public static PropertyList createDefaultProperties(String acronym,
    MasterDataSeed seed) {

    String bid = createBusinessIdentifier(seed, acronym);

    PropertyList properties = new PropertyList();

    properties.set(Constants.SUPERTYPE_KEY,
      Constants.SUPERCLASS_VALUE_MASTER);
    properties.set("num", bid);
    properties.set(Constants.QUALITY, seed.getQuality());
    properties.set(Constants.SOURCEID_KEY, "ERP_" + bid);

    return properties;
  }
}
