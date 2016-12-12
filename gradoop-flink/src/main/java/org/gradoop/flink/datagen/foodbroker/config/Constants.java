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
   * Property key for master data: name.
   */
  public static final String NAME = "name";
  /**
   * Property key for master data: city.
   */
  public static final String CITY = "city";
  /**
   * Property key for master data: gender.
   */
  public static final String GENDER = "gender";
  /**
   * Property key for master data product: category.
   */
  public static final String CATEGORY = "category";
  /**
   * Property key for user: email.
   */
  public static final String EMAIL = "email";
  /**
   * Reserved property key referring to the source identifier of vertices.
   */
  public static final String SOURCEID_KEY = "num";
  /**
   * Property key for master data: quality.
   */
  public static final String QUALITY = "quality";
  /**
   * Property key for product: price property.
   */
  public static final String PRICE = "price";
  /**
   * Property key for transactional vertices: date.
   */
  public static final String DATE = "date";
  /**
   * Property key for transactional vertices: delivery date.
   */
  public static final String DELIVERYDATE = "deliveryDate";
  /**
   * Property key for expense calculated for PurchInvoice.
   */
  public static final String EXPENSE = "expense";
  /**
   * Property key for revenue calculated for SalesInvoice.
   */
  public static final String REVENUE = "revenue";
  /**
   * Property key for line: quantity.
   */
  public static final String QUANTITY = "quantity";
  /**
   * Property key for line: sales price.
   */
  public static final String SALESPRICE = "salesPrice";
  /**
   * Property key for line: purch price.
   */
  public static final String PURCHPRICE = "purchPrice";
  /**
   * Property key for ticket: creation date.
   */
  public static final String CREATEDAT = "createdAt";
  /**
   * Property key for ticket: problem.
   */
  public static final String PROBLEM = "problem";
  /**
   * Property key for ticket: ERP SalesOrder Number.
   */
  public static final String ERPSONUM = "erpSoNum";
  /**
   * Property key for user: ERP Employee Number.
   */
  public static final String ERPEMPLNUM = "erpEmplNum";
  /**
   * Property key for client: ERP Customer Number.
   */
  public static final String ERPCUSTNUM = "erpCustNum";
  /**
   * Property key for client: contact phone.
   */
  public static final String CONTACTPHONE = "contactPhone";
  /**
   * Property key for client: account.
   */
  public static final String ACCOUNT = "account";
  /**
   * Broadcast variable which is needed to spread the precalculated customer map.
   */
  public static final String CUSTOMER_MAP_BC = "customerMap";
  /**
   * Broadcast variable which is needed to spread the precalculated vendor map.
   */
  public static final String VENDOR_MAP_BC = "vendorMap";
  /**
   * Broadcast variable which is needed to spread the precalculated logistic map.
   */
  public static final String LOGISTIC_MAP_BC = "logisticMap";
  /**
   * Broadcast variable which is needed to spread the precalculated employee map.
   */
  public static final String EMPLOYEE_MAP_BC = "employeeMap";
  /**
   * Broadcast variable which is needed to spread the precalculated product quality map.
   */
  public static final String PRODUCT_QUALITY_MAP_BC = "productQualityMap";
  /**
   * Broadcast variable which is needed to spread the precalculated product price map.
   */
  public static final String PRODUCT_PRICE_MAP_BC = "productPriceMap";
  /**
   * Broadcast variable which is needed to spread the .
   */
  public static final String GRAPH_IDS_BC = "graphIds";
  /**
   * Used to select the map which is then used to get the object from a gradoop id.
   */
  public static final String USER_MAP = "userMap";
  /**
   * Acronym for erp process.
   */
  public static final String ERP_ACRONYM = "ERP";
  /**
   * Acronym for cit process.
   */
  public static final String CIT_ACRONYM = "CIT";
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
  /**
   * Acronym for user.
   */
  public static final String USER_ACRONYM = "USE";
  /**
   * Acronym for client.
   */
  public static final String CLIENT_ACRONYM = "CLI";
}
