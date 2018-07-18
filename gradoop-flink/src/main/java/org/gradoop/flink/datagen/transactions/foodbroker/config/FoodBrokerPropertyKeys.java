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
 * Property keys used by the FoodBroker data generator
 */
public class FoodBrokerPropertyKeys {
  /**
   * Reserved property key referring to master or transactional data.
   */
  public static final String SUPERTYPE_KEY = "superType";
  /**
   * Product type fruits.
   */
  public static final String PRODUCT_TYPE_FRUITS = "fruits";
  /**
   * Product type vegetables.
   */
  public static final String PRODUCT_TYPE_VEGETABLES = "vegetables";
  /**
   * Product type nuts.
   */
  public static final String PRODUCT_TYPE_NUTS = "nuts";
  /**
   * Holding type private.
   */
  public static final String HOLDING_TYPE_PRIVATE = "privateHolding";
  /**
   * Property key for master data: name.
   */
  public static final String NAME_KEY = "name";
  /**
   * Property key for master data: city.
   */
  public static final String CITY_KEY = "city";
  /**
   * Property key for master data: state.
   */
  public static final String STATE_KEY = "state";
  /**
   * Property key for master data: country.
   */
  public static final String COUNTRY_KEY = "country";
  /**
   * Property key for master data: branch number.
   */
  public static final String BRANCHNUMBER_KEY = "branchNumber";
  /**
   * Property key for master data: company.
   */
  public static final String COMPANY_KEY = "company";
  /**
   * Property key for master data: holding.
   */
  public static final String HOLDING_KEY = "holding";
  /**
   * Property key for employee master data: employeeType.
   */
  public static final String EMPLOYEE_TYPE_KEY = "employeeType";
  /**
   * Property key for product master data: productType.
   */
  public static final String PRODUCT_TYPE_KEY = "productType";
  /**
   * Property key for master data: gender.
   */
  public static final String GENDER_KEY = "gender";
  /**
   * Property key for master data product: category.
   */
  public static final String CATEGORY_KEY = "category";
  /**
   * Property key for user: email.
   */
  public static final String EMAIL_KEY = "email";
  /**
   * Reserved property key referring to the source identifier of vertices.
   */
  public static final String SOURCEID_KEY = "num";
  /**
   * Property key for master data: quality.
   */
  public static final String QUALITY_KEY = "quality";
  /**
   * Property key for product: price property.
   */
  public static final String PRICE_KEY = "price";
  /**
   * Property key for product: perishableness level.
   */
  public static final String PERISHABLENESS_LEVEL = "perishablenessLevel";
  /**
   * Property key for transactional vertices: date.
   */
  public static final String DATE_KEY = "date";
  /**
   * Property key for transactional vertices: delivery date.
   */
  public static final String DELIVERYDATE_KEY = "deliveryDate";
  /**
   * Property key for expense calculated for PurchInvoice.
   */
  public static final String EXPENSE_KEY = "expense";
  /**
   * Property key for revenue calculated for SalesInvoice.
   */
  public static final String REVENUE_KEY = "revenue";
  /**
   * Property key for line: quantity.
   */
  public static final String QUANTITY_KEY = "quantity";
  /**
   * Property key for line: sales price.
   */
  public static final String SALESPRICE_KEY = "salesPrice";
  /**
   * Property key for line: purch price.
   */
  public static final String PURCHPRICE_KEY = "purchPrice";
  /**
   * Property key for ticket: creation date.
   */
  public static final String CREATEDATE_KEY = "createdAt";
  /**
   * Property key for ticket: problem.
   */
  public static final String PROBLEM_KEY = "problem";
  /**
   * Property key for ticket: ERP SalesOrder Number.
   */
  public static final String ERPSONUM_KEY = "erpSoNum";
  /**
   * Property key for user: ERP Employee Number.
   */
  public static final String ERPEMPLNUM_KEY = "erpEmplNum";
  /**
   * Property key for client: ERP Customer Number.
   */
  public static final String ERPCUSTNUM_KEY = "erpCustNum";
  /**
   * Property key for client: contact phone.
   */
  public static final String CONTACTPHONE_KEY = "contactPhone";
  /**
   * Property key for client: account.
   */
  public static final String ACCOUNT_KEY = "account";
  /**
   * Property key for purch and sales invoice.
   */
  public static final String TEXT_KEY = "text";
}
