/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.datagen.transactions.foodbroker.functions.masterdata;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerAcronyms;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerPropertyKeys;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerPropertyValues;
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

    properties
      .set(FoodBrokerPropertyKeys.SUPERTYPE_KEY, FoodBrokerPropertyValues.SUPERCLASS_VALUE_MASTER);
    properties
      .set(FoodBrokerPropertyKeys.QUALITY_KEY, seed.getQuality());
    properties
      .set(FoodBrokerPropertyKeys.SOURCEID_KEY, FoodBrokerAcronyms.ERP_ACRONYM + "_" + bid);

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
