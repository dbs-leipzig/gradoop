
package org.gradoop.flink.datagen.transactions.foodbroker.functions.masterdata;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.datagen.transactions.foodbroker.config.Constants;
import org.gradoop.flink.datagen.transactions.foodbroker.tuples.MasterDataSeed;

/**
 * Provides default properties and a business identifier for master data objects.
 */
public abstract class MasterData extends RichMapFunction<MasterDataSeed, Vertex> {
  /**
   * Creates a business identifier.
   *
   * @param seed the master data seed
   * @param acronym the master data acronym
   * @return a business identifier
   */
  private static String createBusinessIdentifier(MasterDataSeed seed, String acronym) {
    String idString = String.valueOf(seed.getNumber());
    long count = 8 - idString.length();
    // set preceding zeros
    for (int i = 1; i <= count; i++) {
      idString = "0" + idString;
    }
    return acronym + idString;
  }

  /**
   * Creates default properties for a master data object.
   *
   * @param seed the master data seed
   * @param acronym the master data acronym
   * @return property list with default master data properties
   */
  protected Properties createDefaultProperties(MasterDataSeed seed, String acronym) {
    String bid = createBusinessIdentifier(seed, acronym);
    Properties properties = new Properties();

    properties.set(Constants.SUPERTYPE_KEY, Constants.SUPERCLASS_VALUE_MASTER);
    properties.set(Constants.QUALITY_KEY, seed.getQuality());
    properties.set(Constants.SOURCEID_KEY, Constants.ERP_ACRONYM + "_" + bid);

    return properties;
  }

  /**
   * Returns the acronym of the person.
   *
   * @return acronym
   */
  public abstract String getAcronym();

  /**
   * Returns the class name of the person.
   *
   * @return class name
   */
  public abstract String getClassName();
}
