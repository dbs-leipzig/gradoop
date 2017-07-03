
package org.gradoop.flink.model.impl.functions.epgm;

import com.google.common.collect.Lists;
import org.gradoop.flink.model.api.functions.Function;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Extracts a list of {@link PropertyValue} instances from a given entity using
 * a list of property keys. The order of the property keys determines the order
 * of the values in the result. If the EPGM element does not have a property,
 * the property value will be {@code PropertyValue.NULL_VALUE}.
 *
 * @param <EL> EPGM element
 */
public class PropertyGetter<EL extends Element>
  implements Function<EL, List<PropertyValue>> {

  /**
   * Property keys to retrieve property values
   */
  private final List<String> propertyKeys;

  /**
   * Constructor
   *
   * @param propertyKeys property keys to retrieve values for
   */
  public PropertyGetter(List<String> propertyKeys) {
    this.propertyKeys = checkNotNull(propertyKeys);
  }

  @Override
  public List<PropertyValue> apply(EL entity) {
    List<PropertyValue> propertyValueList =
      Lists.newArrayListWithCapacity(propertyKeys.size());

    for (String propertyKey : propertyKeys) {
      if (entity.hasProperty(propertyKey)) {
        propertyValueList.add(entity.getPropertyValue(propertyKey));
      } else {
        propertyValueList.add(PropertyValue.NULL_VALUE);
      }
    }
    return propertyValueList;
  }
}
