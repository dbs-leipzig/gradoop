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
 * Configuration keys names used by the FoodBroker data generator
 */
public class FoodBrokerConfigurationKeys {
  /**
   * Key for configuration property.
   */
  public static final String SQ_CONFIRMATIONPROBABILITY_CONFIG_KEY = "confirmationProbability";
  /**
   * Key for configuration property.
   */
  public static final String SQ_LINES_CONFIG_KEY = "lines";
  /**
   * Key for configuration property.
   */
  public static final String SQ_SALESMARGIN_CONFIG_KEY = "salesMargin";
  /**
   * Key for configuration property.
   */
  public static final String SQ_LINEQUANTITY_CONFIG_KEY = "lineQuantity";
  /**
   * Key for configuration property.
   */
  public static final String SQ_CONFIRMATIONDELAY_CONFIG_KEY = "confirmationDelay";
  /**
   * Key for configuration property.
   */
  public static final String SO_DELIVERYAGREEMENTDELAY_CONFIG_KEY = "deliveryAgreementDelay";
  /**
   * Key for configuration property.
   */
  public static final String PO_NUMBEROFVENDORS_CONFIG_KEY = "numberOfVendors";
  /**
   * Key for configuration property.
   */
  public static final String PO_PURCHASEDELAY_CONFIG_KEY = "purchaseDelay";
  /**
   * Key for configuration property.
   */
  public static final String PO_PRICEVARIATION_CONFIG_KEY = "priceVariation";
  /**
   * Key for configuration property.
   */
  public static final String PO_DELIVERYDELAY_CONFIG_KEY = "deliveryDelay";
  /**
   * Key for configuration property.
   */
  public static final String PO_INVOICEDELAY_CONFIG_KEY = "invoiceDelay";
  /**
   * Key for configuration property.
   */
  public static final String SO_INVOICEDELAY_CONFIG_KEY = "invoiceDelay";
  /**
   * Key for configuration property.
   */
  public static final String TI_BADQUALITYPROBABILITY_CONFIG_KEY = "badQualityProbability";
  /**
   * Key for configuration property.
   */
  public static final String TI_SALESREFUND_CONFIG_KEY = "salesRefund";
  /**
   * Key for configuration property.
   */
  public static final String TI_PURCHREFUND_CONFIG_KEY = "purchRefund";
}
