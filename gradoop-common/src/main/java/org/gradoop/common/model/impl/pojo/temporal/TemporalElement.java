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
package org.gradoop.common.model.impl.pojo.temporal;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * Base class for all temporal elements. Contains interval definitions for transaction time and
 * valid time.
 */
public abstract class TemporalElement extends Element implements EPGMElement {

  /**
   * The default value for unset valid times.
   */
  public static final Long DEFAULT_VALID_TIME = 0L;
  /**
   * Transaction time interval containing the beginning and end of the elements transaction time.
   * Its values are unix timestamps in milliseconds.
   */
  private Tuple2<Long, Long> transactionTime;
  /**
   * Valid time interval containing the beginning and end of the elements validity.
   * Its values are unix timestamps in milliseconds.
   */
  private Tuple2<Long, Long> validTime;

  /**
   * Default constructor.
   */
  public TemporalElement() {
    transactionTime = new Tuple2<>();
    validTime = new Tuple2<>();
  }

  /**
   * Creates a new temporal graph element.
   *
   * @param id the element identifier
   * @param label the element label
   * @param properties the element properties
   * @param validFrom the beginning of the elements validity as unix timestamp in milliseconds
   * @param validTo the end of the elements validity as unix timestamp in milliseconds
   */
  TemporalElement(GradoopId id, String label, Properties properties, Long validFrom,
    Long validTo) {
    super(id, label, properties);
    // Set transaction time beginning to the current system time
    transactionTime = new Tuple2<>(System.currentTimeMillis(), Long.MAX_VALUE);

    validTime = new Tuple2<>(DEFAULT_VALID_TIME, DEFAULT_VALID_TIME);
    if (validFrom != null) {
      this.setValidFrom(validFrom);
    }

    if (validTo != null) {
      this.setValidTo(validTo);
    }
  }

  /**
   * Get the transaction time tuple (tx-from, tx-to). Needed because of Flink's POJO rules.
   *
   * @return a tuple 2 representing the transaction time interval
   */
  public Tuple2<Long, Long> getTransactionTime() {
    return transactionTime;
  }

  /**
   * Set the transaction time tuple (tx-from, tx-to). Needed because of Flink's POJO rules.
   * @param transactionTime a tuple 2 representing the transaction time interval
   */
  public void setTransactionTime(Tuple2<Long, Long> transactionTime) {
    this.transactionTime = transactionTime;
  }

  /**
   * Get the valid time tuple (valid-from, valid-to). Needed because of Flink's POJO rules.
   *
   * @return a tuple 2 representing the valid time interval
   */
  public Tuple2<Long, Long> getValidTime() {
    return validTime;
  }

  /**
   * Set the valid time tuple (valid-from, valid-to). Needed because of Flink's POJO rules.
   * @param validTime a tuple 2 representing the valid time interval
   */
  public void setValidTime(Tuple2<Long, Long> validTime) {
    this.validTime = validTime;
  }

  /**
   * Get the beginning of the elements validity as unix timestamp in milliseconds.
   *
   * @return the beginning of the elements validity as unix timestamp in milliseconds
   */
  public Long getValidFrom() {
    return this.validTime.f0;
  }

  /**
   * Sets the beginning of the elements validity as unix timestamp in milliseconds.
   *
   * @param validFrom the beginning of the elements validity as unix timestamp in milliseconds
   */
  public void setValidFrom(Long validFrom) {
    this.validTime.f0 = validFrom;
  }

  /**
   * Get the end of the elements validity as unix timestamp in milliseconds.
   *
   * @return the end of the elements validity as unix timestamp in milliseconds
   */
  public Long getValidTo() {
    return this.validTime.f1;
  }

  /**
   * Sets the end of the elements validity as unix timestamp in milliseconds.
   *
   * @param validTo the end of the elements validity as unix timestamp in milliseconds
   */
  public void setValidTo(Long validTo) {
    this.validTime.f1 = validTo;
  }

  /**
   * Get the beginning of the elements transaction interval as unix timestamp in milliseconds.
   *
   * @return the beginning of the elements transaction interval as unix timestamp in milliseconds
   */
  public Long getTxFrom() {
    return this.transactionTime.f0;
  }

  /**
   * Get the end of the elements transaction interval as unix timestamp in milliseconds.
   *
   * @return the end of the elements transaction interval as unix timestamp in milliseconds
   */
  public Long getTxTo() {
    return this.transactionTime.f1;
  }
}
