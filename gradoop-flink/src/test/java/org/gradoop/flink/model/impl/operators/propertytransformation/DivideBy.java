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
package org.gradoop.flink.model.impl.operators.propertytransformation;

import org.gradoop.common.model.impl.properties.PropertyValue;	
import org.gradoop.flink.model.api.functions.PropertyTransformationFunction;	

/**
 * A sample property transformation function which can be used for the property transformation
 * operator.
 *
 * @see org.gradoop.flink.model.impl.operators.propertytransformation.PropertyTransformation
 * @see org.gradoop.flink.model.api.functions.PropertyTransformationFunction
 */
public class DivideBy implements PropertyTransformationFunction {	
  /**	
   * Avoid object instantiation.	
   */	
  private PropertyValue reuseProperty = new PropertyValue();	
  /**	
   * Value which the property value shall be divided by.	
   */	
  private long divisor;	
  
  /**	
   * Creates a new instance of DivideBy which can be used for property transformation.
   *	
   * @param divisor divisor for the property value	
   */	
  public DivideBy(long divisor) {	
    this.divisor = divisor;	
  }	
  
  @Override	
  public PropertyValue execute(PropertyValue property) {	
    reuseProperty.setLong(property.getLong() / divisor);	
    return reuseProperty;	
  }	
}