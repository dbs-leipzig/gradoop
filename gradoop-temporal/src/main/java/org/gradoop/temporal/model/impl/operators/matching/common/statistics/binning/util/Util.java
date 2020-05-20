package org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.util;

import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Provides utility methods that are needed in several binning classes
 */
public class Util {
    /**
     * Converts a numerical PropertyValue to its value as a double
     * @param value PropertyValue to convert
     * @param cls type of the PropertyValue
     * @return double representation of value
     */
    public static double propertyValueToDouble(PropertyValue value, Class cls){
        if(cls.equals(Integer.class)){
            return (double) value.getInt();
        }
        if(cls.equals(Double.class)){
            return value.getDouble();
        }
        if(cls.equals(Long.class)){
            return (double) value.getLong();
        }
        if(cls.equals(Float.class)){
            return Double.parseDouble(value.toString());
        }
        return 0.;
    }

    public static double propertyValueToDouble(PropertyValue value){
        return propertyValueToDouble(value, value.getType());
    }
}
