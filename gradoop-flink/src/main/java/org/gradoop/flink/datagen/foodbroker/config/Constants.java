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
package org.gradoop.flink.datagen.foodbroker.config;

/**
 * Constants used for the FoodBroker data generator
 */
public class Constants {
  /**
   * Reserved property key referring to master or transactional data.
   */
  public static final String SUPERTYPE_KEY = "superType";
  /**
   * Reserved property value to mark master data.
   */
  public static final String SUPERCLASS_VALUE_MASTER = "M";
  /**
   * Reserved property value to mark transactional data.
   */
  public static final String SUPERCLASS_VALUE_TRANSACTIONAL = "T";
  /**
   * Reserved property key referring to the source identifier of vertices.
   */
  public static final String SOURCEID_KEY = "num";
  /**
   * Property key for master data quality.
   */
  public static final String QUALITY = "quality";
  /**
   * Property key for product price.
   */
  public static final String PRICE = "price";
  /**
   * Broadcast variable which is needed to spread the precalculated customer map.
   */
  public static final String CUSTOMER_MAP = "customerMap";
  /**
   * Broadcast variable which is needed to spread the precalculated vendor map.
   */
  public static final String VENDOR_MAP = "vendorMap";
  /**
   * Broadcast variable which is needed to spread the precalculated logistic map.
   */
  public static final String LOGISTIC_MAP = "logisticMap";
  /**
   * Broadcast variable which is needed to spread the precalculated employee map.
   */
  public static final String EMPLOYEE_MAP = "employeeMap";
  /**
   * Broadcast variable which is needed to spread the precalculated product quality map.
   */
  public static final String PRODUCT_QUALITY_MAP = "productQualityMap";
  /**
   * Broadcast variable which is needed to spread the precalculated product price map.
   */
  public static final String PRODUCT_PRICE_MAP = "productPriceMap";
  /**
   * Used to select the map which is then used to get the object from a gradoop id.
   */
  public static final String USER_MAP = "userMap";
  /**
   * Acronym for sales quotation.
   */
  public static final String SALESQUOTATION_ACRONYM = "SQN";
  /**
   * Acronym for sales order.
   */
  public static final String SALESORDER_ACRONYM = "SOR";
  /**
   * Acronym for purch order.
   */
  public static final String PURCHORDER_ACRONYM = "POR";
  /**
   * Acronym for delivery note.
   */
  public static final String DELIVERYNOTE_ACRONYM = "DLV";
  /**
   * Acronym for purch invoice.
   */
  public static final String PURCHINVOICE_ACRONYM = "PIN";
  /**
   * Acronym for sales invoice.
   */
  public static final String SALESINVOICE_ACRONYM = "SIN";
}
