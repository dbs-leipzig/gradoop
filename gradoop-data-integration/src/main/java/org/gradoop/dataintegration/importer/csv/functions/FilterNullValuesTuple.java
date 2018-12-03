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
package org.gradoop.dataintegration.importer.csv.functions;

import org.apache.flink.api.common.functions.FilterFunction;

/**
 * FilterFunction to remove all tuple that return null.
 * Null value tuple are stored, if the csv file contains a header.
 * To not import this line/tuple as an vertex/edge, we set it null and filter it.
 *
 * @param <T> The type of the filtered elements.
 */
public class FilterNullValuesTuple<T> implements FilterFunction<T> {

  @Override
  public boolean filter(final T value) {
    return value != null;
  }
}
