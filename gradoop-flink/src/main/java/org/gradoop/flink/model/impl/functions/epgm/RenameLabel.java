/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.functions.epgm;

import org.gradoop.common.model.api.entities.Element;
import org.gradoop.flink.model.api.functions.TransformationFunction;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A serializable function that is applied on an element (i.e. graph head,
 * vertex and edge) to rename a label
 *
 * @param <T> the {@link Element} which is target of change.
 */
public class RenameLabel<T extends Element> implements TransformationFunction<T> {

  /**
   * the old label which will be renamed by the process
   */
  private final String oldLabel;

  /**
   * the new label which will be taken as replacement for the old one
   */
  private final String newLabel;

  /**
   * Constructor
   *
   * @param oldLabel the label to be renamed
   * @param newLabel the new label replacing the old one
   */
  public RenameLabel(String oldLabel, String newLabel) {
    this.oldLabel = checkNotNull(oldLabel);
    this.newLabel = checkNotNull(newLabel);
  }

  @Override
  public T apply(T current, T transformed) {

    if (current.getLabel().equals(oldLabel)) {
      current.setLabel(newLabel);
    }

    return current;
  }
}
