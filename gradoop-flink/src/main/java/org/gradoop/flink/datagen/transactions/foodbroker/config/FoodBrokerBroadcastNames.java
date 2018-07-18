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
package org.gradoop.flink.datagen.transactions.foodbroker.config;

/**
 * Broadcast names used by the FoodBroker data generator
 */
public class FoodBrokerBroadcastNames {
  /**
   * Broadcast variable for the customers adjectives.
   */
  public static final String ADJECTIVES_BC = "adjectives";
  /**
   * Broadcast variable for the customers nouns.
   */
  public static final String NOUNS_BC = "nouns";
  /**
   * Broadcast variable for the logistics cities.
   */
  public static final String CITIES_BC = "cities";
  /**
   * Broadcast variable for male employees first name.
   */
  public static final String FIRST_NAMES_MALE_BC = "firstNamesMale";
  /**
   * Broadcast variable for female employees first name.
   */
  public static final String FIRST_NAMES_FEMALE_BC = "firstNamesFemale";
  /**
   * Broadcast variable for employees last name.
   */
  public static final String LAST_NAMES_BC = "nouns";
  /**
   * Broadcast variable for product names.
   */
  public static final String NAMES_GROUPS_BC = "nameGroupPairs";
  /**
   * Broadcast variable for the customers companies.
   */
  public static final String COMPANIES_BC = "companies";
  /**
   * Broadcast variable for the customers companies holdings.
   */
  public static final String HOLDINGS_BC = "holdings";
  /**
   * Broadcast variable which is needed to spread the precalculated customer map.
   */
  public static final String BC_CUSTOMERS = "customerMap";
  /**
   * Broadcast variable which is needed to spread the precalculated vendor map.
   */
  public static final String BC_VENDORS = "vendorMap";
  /**
   * Broadcast variable which is needed to spread the precalculated logistic map.
   */
  public static final String BC_LOGISTICS = "logisticMap";
  /**
   * Broadcast variable which is needed to spread the precalculated employee map.
   */
  public static final String BC_EMPLOYEES = "employeeIndex";
  /**
   * Broadcast variable which is needed to spread the precalculated product price map.
   */
  public static final String BC_PRODUCTS = "productPriceMap";
  /**
   * Used to select the map which is then used to get the object from a gradoop id.
   */
  public static final String USER_MAP = "userMap";
}
