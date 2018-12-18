package org.gradoop.common.model.impl.properties.strategies;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.api.strategies.PropertyValueStrategy;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PropertyValueStrategyFactory {

  private static PropertyValueStrategyFactory INSTANCE = new PropertyValueStrategyFactory();
  private final Map<Class, PropertyValueStrategy> classStrategyMap;
  private final Map<Byte, PropertyValueStrategy> byteStrategyMap;
  private final NoopPropertyValueStrategy noopPropertyValueStrategy = new NoopPropertyValueStrategy();

  private PropertyValueStrategyFactory() {
    classStrategyMap = new HashMap<>();
    classStrategyMap.put(Boolean.class, new BooleanStrategy());
    classStrategyMap.put(Set.class, new SetStrategy());
    classStrategyMap.put(Integer.class, new IntegerStrategy());
    classStrategyMap.put(Long.class, new LongStrategy());
    classStrategyMap.put(Float.class, new FloatStrategy());
    classStrategyMap.put(Double.class, new DoubleStrategy());
    classStrategyMap.put(Short.class, new ShortStrategy());
    classStrategyMap.put(BigDecimal.class, new BigDecimalStrategy());
    classStrategyMap.put(LocalDate.class, new DateStrategy());
    classStrategyMap.put(LocalTime.class, new TimeStrategy());
    classStrategyMap.put(LocalDateTime.class, new DateTimeStrategy());
    classStrategyMap.put(GradoopId.class, new GradoopIdStrategy());
    classStrategyMap.put(String.class, new StringStrategy());
    classStrategyMap.put(List.class, new ListStrategy());
    classStrategyMap.put(Map.class, new MapStrategy());

    byteStrategyMap = new HashMap<>(classStrategyMap.size());
    for (PropertyValueStrategy strategy : classStrategyMap.values()) {
      byteStrategyMap.put(strategy.getRawType(), strategy);
    }
  }

  public static PropertyValueStrategy get(Class c) {
    PropertyValueStrategy strategy = INSTANCE.classStrategyMap.get(c);
    if (strategy == null) {
      for (Map.Entry<Class, PropertyValueStrategy> entry : INSTANCE.classStrategyMap.entrySet()) {
        if (entry.getKey().isAssignableFrom(c)) {
          strategy = entry.getValue();
          INSTANCE.classStrategyMap.put(c, strategy);
          break;
        }
      }
    }
    return strategy == null ? INSTANCE.noopPropertyValueStrategy : strategy;
  }

  public static Object fromRawBytes(byte[] bytes) {
    PropertyValueStrategy strategy = INSTANCE.byteStrategyMap.get(bytes[0]);
    return strategy == null ? null : strategy.get(bytes);
  }

  public static int compare(Object value, Object other) {
    if (value != null) {
      PropertyValueStrategy strategy = get(value.getClass());
      return strategy.compare(value, other);
    }

    return 0;
  }

  public static byte[] getRawBytes(Object value) {
    if (value != null) {
      return get(value.getClass()).getRawBytes(value);
    }
    return new byte[0];
  }

  public static PropertyValueStrategy get(byte value) {
    return INSTANCE.byteStrategyMap.get(value);
  }

  public static PropertyValueStrategy get(Object value) {
    if (value != null) {
      return get(value.getClass());
    }
    return INSTANCE.noopPropertyValueStrategy;
  }
}
