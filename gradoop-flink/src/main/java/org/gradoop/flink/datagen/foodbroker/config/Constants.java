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
   * reserved property key referring to master or transactional data
   */
  public static final String SUPERTYPE_KEY = "superType";
  /**
   * reserved property value to mark master data
   */
  public static final String SUPERCLASS_VALUE_MASTER = "M";
  /**
   * reserved property value to mark transactional data
   */
  public static final String SUPERCLASS_VALUE_TRANSACTIONAL = "T";
  /**
   * reserved property key referring to the source identifier of vertices
   */
  public static final String SOURCEID_KEY = "sid";

  /**
   * property key for master data quality
   */
  public static final String QUALITY = "quality";

  /**
   * property key for product price
   */
  public static final String PRICE = "price";

  /**
   * broadcast variable which is needed to find and set all graph ids for the
   * masterdata
   */
  public static final String EDGES = "edges";

  /**
   * broadcast variable which is needed spread the precalculated customer map
   */
  public static final String CUSTOMER_MAP = "customerMap";

  public static final String VENDOR_MAP = "vendorMap";

  public static final String LOGISTIC_MAP = "logisticMap";

  public static final String EMPLOYEE_MAP = "employeeMap";

  public static final String PRODUCT_QUALITY_MAP = "productQualityMap";

  public static final String PRODUCT_PRICE_MAP = "productPriceMap";

  public static final String USER_MAP = "userMap";

  public static final String VERTEX_MAP = "vertexMap";

  public static final String EDGE_MAP = "edgeMap";

  public static final String SALESQUOTATION_ACRONYM = "SQN";
  public static final String SALESORDER_ACRONYM = "SOR";
  public static final String PURCHORDER_ACRONYM = "POR";
  public static final String DELIVERYNOTE_ACRONYM = "DLV";
  public static final String PURCHINVOICE_ACRONYM = "PIN";
  public static final String SALESINVOICE_ACRONYM = "PIN";
}
