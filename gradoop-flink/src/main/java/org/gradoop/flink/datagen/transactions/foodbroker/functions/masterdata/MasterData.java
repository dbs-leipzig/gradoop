/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.datagen.transactions.foodbroker.functions.masterdata;

import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.datagen.transactions.foodbroker.config.Constants;
import org.gradoop.flink.datagen.transactions.foodbroker.tuples.MasterDataSeed;

/**
 * Provides default properties and a business identifier for master data objects.
 */
public class MasterData {
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
  public static Properties createDefaultProperties(MasterDataSeed seed, String acronym) {
    String bid = createBusinessIdentifier(seed, acronym);
    Properties properties = new Properties();

    properties.set(Constants.SUPERTYPE_KEY, Constants.SUPERCLASS_VALUE_MASTER);
    properties.set(Constants.QUALITY_KEY, seed.getQuality());
    properties.set(Constants.SOURCEID_KEY, Constants.ERP_ACRONYM + "_" + bid);

    return properties;
  }
}
