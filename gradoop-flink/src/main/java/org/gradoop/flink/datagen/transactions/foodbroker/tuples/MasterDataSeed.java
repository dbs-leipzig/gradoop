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
package org.gradoop.flink.datagen.transactions.foodbroker.tuples;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Tuple which contains the basic information of a master data object i.e. the sequence number
 * and its quality.
 */
public class MasterDataSeed extends Tuple2<Integer, Float> {

  /**
   * Default constructor.
   */
  public MasterDataSeed() {
  }

  /**
   * Valued constructor.
   *
   * @param number sequence number
   * @param quality master data quality
   */
  public MasterDataSeed(Integer number, Float quality) {
    super(number, quality);
  }

  public Integer getNumber() {
    return this.f0;
  }

  public Float getQuality() {
    return this.f1;
  }
}
