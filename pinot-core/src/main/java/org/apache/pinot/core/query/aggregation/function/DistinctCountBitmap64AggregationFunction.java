package org.apache.pinot.core.query.aggregation.function;

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.Roaring64BitmapUtils;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.longlong.PeekableLongIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;


public class DistinctCountBitmap64AggregationFunction extends BaseSingleInputAggregationFunction<Roaring64Bitmap, Long> {

  public DistinctCountBitmap64AggregationFunction(List<ExpressionContext> arguments) {
    this(verifySingleArgument(arguments, "DISTINCT_COUNT_BITMAP64"));
  }

  protected DistinctCountBitmap64AggregationFunction(ExpressionContext expression) {
    super(expression);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTBITMAP64;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
    Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // Treat BYTES value as serialized RoaringBitmap
    FieldSpec.DataType storedType = blockValSet.getValueType().getStoredType();
    if (storedType == FieldSpec.DataType.BYTES) {
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      Roaring64Bitmap valueBitmap = aggregationResultHolder.getResult();
      if (valueBitmap != null) {
        for (int i = 0; i < length; i++) {
          valueBitmap.or(Roaring64BitmapUtils.deserialize(bytesValues[i]));
        }
      } else {
        valueBitmap = Roaring64BitmapUtils.deserialize(bytesValues[0]);
        aggregationResultHolder.setValue(valueBitmap);
        for (int i = 1; i < length; i++) {
          valueBitmap.or(Roaring64BitmapUtils.deserialize(bytesValues[i]));
        }
      }
      return;
    }

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      Roaring64Bitmap dictIdBitmap = getDictIdBitmap(aggregationResultHolder, dictionary);
      for (int i = 0; i < length; i++) {
        dictIdBitmap.add(dictIds[i]);
      }
      return;
    }

    // For non-dictionary-encoded expression, store hash code of the values into the bitmap
    Roaring64Bitmap valueBitmap = getValueBitmap(aggregationResultHolder);
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          valueBitmap.add(intValues[i]);
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          valueBitmap.add(Long.hashCode(longValues[i]));
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          valueBitmap.add(Float.hashCode(floatValues[i]));
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          valueBitmap.add(Double.hashCode(doubleValues[i]));
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          valueBitmap.add(stringValues[i].hashCode());
        }
        break;
      default:
        throw new IllegalStateException(
          "Illegal data type for DISTINCT_COUNT_BITMAP64 aggregation function: " + storedType);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
    Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // Treat BYTES value as serialized RoaringBitmap
    FieldSpec.DataType storedType = blockValSet.getValueType().getStoredType();
    if (storedType == FieldSpec.DataType.BYTES) {
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      for (int i = 0; i < length; i++) {
        Roaring64Bitmap value = Roaring64BitmapUtils.deserialize(bytesValues[i]);
        int groupKey = groupKeyArray[i];
        Roaring64Bitmap valueBitmap = groupByResultHolder.getResult(groupKey);
        if (valueBitmap != null) {
          valueBitmap.or(value);
        } else {
          groupByResultHolder.setValueForKey(groupKey, value);
        }
      }
      return;
    }

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      for (int i = 0; i < length; i++) {
        getDictIdBitmap(groupByResultHolder, groupKeyArray[i], dictionary).add(dictIds[i]);
      }
      return;
    }

    // For non-dictionary-encoded expression, store hash code of the values into the bitmap
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          getValueBitmap(groupByResultHolder, groupKeyArray[i]).add(intValues[i]);
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          getValueBitmap(groupByResultHolder, groupKeyArray[i]).add(Long.hashCode(longValues[i]));
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          getValueBitmap(groupByResultHolder, groupKeyArray[i]).add(Float.hashCode(floatValues[i]));
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          getValueBitmap(groupByResultHolder, groupKeyArray[i]).add(Double.hashCode(doubleValues[i]));
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          getValueBitmap(groupByResultHolder, groupKeyArray[i]).add(stringValues[i].hashCode());
        }
        break;
      default:
        throw new IllegalStateException(
          "Illegal data type for DISTINCT_COUNT_BITMAP64 aggregation function: " + storedType);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
    Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // Treat BYTES value as serialized RoaringBitmap
    FieldSpec.DataType storedType = blockValSet.getValueType().getStoredType();
    if (storedType == FieldSpec.DataType.BYTES) {
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      for (int i = 0; i < length; i++) {
        Roaring64Bitmap value = Roaring64BitmapUtils.deserialize(bytesValues[i]);
        for (int groupKey : groupKeysArray[i]) {
          Roaring64Bitmap bitmap = groupByResultHolder.getResult(groupKey);
          if (bitmap != null) {
            bitmap.or(value);
          } else {
            // Clone a bitmap for the group
            groupByResultHolder.setValueForKey(groupKey, value.clone());
          }
        }
      }
      return;
    }

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      for (int i = 0; i < length; i++) {
        setDictIdForGroupKeys(groupByResultHolder, groupKeysArray[i], dictionary, dictIds[i]);
      }
      return;
    }

    // For non-dictionary-encoded expression, store hash code of the values into the bitmap
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], intValues[i]);
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], Long.hashCode(longValues[i]));
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], Float.hashCode(floatValues[i]));
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], Double.hashCode(doubleValues[i]));
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], stringValues[i].hashCode());
        }
        break;
      default:
        throw new IllegalStateException(
          "Illegal data type for DISTINCT_COUNT_BITMAP64 aggregation function: " + storedType);
    }
  }

  @Override
  public Roaring64Bitmap extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    Object result = aggregationResultHolder.getResult();
    if (result == null) {
      return new Roaring64Bitmap();
    }

    if (result instanceof DistinctCountBitmap64AggregationFunction.DictIdsWrapper) {
      // For dictionary-encoded expression, convert dictionary ids to hash code of the values
      return convertToValueBitmap((DistinctCountBitmap64AggregationFunction.DictIdsWrapper) result);
    } else {
      // For serialized RoaringBitmap and non-dictionary-encoded expression, directly return the value bitmap
      return (Roaring64Bitmap) result;
    }
  }

  @Override
  public Roaring64Bitmap extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    Object result = groupByResultHolder.getResult(groupKey);
    if (result == null) {
      return new Roaring64Bitmap();
    }

    if (result instanceof DistinctCountBitmap64AggregationFunction.DictIdsWrapper) {
      // For dictionary-encoded expression, convert dictionary ids to hash code of the values
      return convertToValueBitmap((DistinctCountBitmap64AggregationFunction.DictIdsWrapper) result);
    } else {
      // For serialized RoaringBitmap and non-dictionary-encoded expression, directly return the value bitmap
      return (Roaring64Bitmap) result;
    }
  }

  @Override
  public Roaring64Bitmap merge(Roaring64Bitmap intermediateResult1, Roaring64Bitmap intermediateResult2) {
    intermediateResult1.or(intermediateResult2);
    return intermediateResult1;
  }

  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.OBJECT;
  }

  @Override
  public AggregationFunction.SerializedIntermediateResult serializeIntermediateResult(Roaring64Bitmap roaringBitmap) {
    return new AggregationFunction.SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.Roaring64Bitmap.getValue(),
      ObjectSerDeUtils.ROARING_BITMAP64_SER_DE.serialize(roaringBitmap));
  }

  @Override
  public Roaring64Bitmap deserializeIntermediateResult(CustomObject customObject) {
    return ObjectSerDeUtils.ROARING_BITMAP64_SER_DE.deserialize(customObject.getBuffer());
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.LONG;
  }

  @Override
  public Long extractFinalResult(Roaring64Bitmap intermediateResult) {
    return intermediateResult.getLongCardinality();
  }

  @Override
  public Long mergeFinalResult(Long finalResult1, Long finalResult2) {
    return finalResult1 + finalResult2;
  }

  /**
   * Returns the dictionary id bitmap from the result holder or creates a new one if it does not exist.
   */
  protected static Roaring64Bitmap getDictIdBitmap(AggregationResultHolder aggregationResultHolder,
    Dictionary dictionary) {
    DistinctCountBitmap64AggregationFunction.DictIdsWrapper dictIdsWrapper = aggregationResultHolder.getResult();
    if (dictIdsWrapper == null) {
      dictIdsWrapper = new DistinctCountBitmap64AggregationFunction.DictIdsWrapper(dictionary);
      aggregationResultHolder.setValue(dictIdsWrapper);
    }
    return dictIdsWrapper._dictIdBitmap;
  }

  /**
   * Returns the value bitmap from the result holder or creates a new one if it does not exist.
   */
  protected static Roaring64Bitmap getValueBitmap(AggregationResultHolder aggregationResultHolder) {
    Roaring64Bitmap bitmap = aggregationResultHolder.getResult();
    if (bitmap == null) {
      bitmap = new Roaring64Bitmap();
      aggregationResultHolder.setValue(bitmap);
    }
    return bitmap;
  }

  /**
   * Returns the dictionary id bitmap for the given group key or creates a new one if it does not exist.
   */
  protected static Roaring64Bitmap getDictIdBitmap(GroupByResultHolder groupByResultHolder, int groupKey,
    Dictionary dictionary) {
    DistinctCountBitmap64AggregationFunction.DictIdsWrapper dictIdsWrapper = groupByResultHolder.getResult(groupKey);
    if (dictIdsWrapper == null) {
      dictIdsWrapper = new DistinctCountBitmap64AggregationFunction.DictIdsWrapper(dictionary);
      groupByResultHolder.setValueForKey(groupKey, dictIdsWrapper);
    }
    return dictIdsWrapper._dictIdBitmap;
  }

  /**
   * Returns the value bitmap for the given group key or creates a new one if it does not exist.
   */
  protected static Roaring64Bitmap getValueBitmap(GroupByResultHolder groupByResultHolder, int groupKey) {
    Roaring64Bitmap bitmap = groupByResultHolder.getResult(groupKey);
    if (bitmap == null) {
      bitmap = new Roaring64Bitmap();
      groupByResultHolder.setValueForKey(groupKey, bitmap);
    }
    return bitmap;
  }

  /**
   * Helper method to set dictionary id for the given group keys into the result holder.
   */
  private static void setDictIdForGroupKeys(GroupByResultHolder groupByResultHolder, int[] groupKeys,
    Dictionary dictionary, int dictId) {
    for (int groupKey : groupKeys) {
      getDictIdBitmap(groupByResultHolder, groupKey, dictionary).add(dictId);
    }
  }

  /**
   * Helper method to set value for the given group keys into the result holder.
   */
  private void setValueForGroupKeys(GroupByResultHolder groupByResultHolder, int[] groupKeys, int value) {
    for (int groupKey : groupKeys) {
      getValueBitmap(groupByResultHolder, groupKey).add(value);
    }
  }

  /**
   * Helper method to read dictionary and convert dictionary ids to hash code of the values for dictionary-encoded
   * expression.
   */
  private static Roaring64Bitmap convertToValueBitmap(DistinctCountBitmap64AggregationFunction.DictIdsWrapper dictIdsWrapper) {
    Dictionary dictionary = dictIdsWrapper._dictionary;
    Roaring64Bitmap dictIdBitmap = dictIdsWrapper._dictIdBitmap;
    Roaring64Bitmap valueBitmap = new Roaring64Bitmap();
    PeekableLongIterator iterator = dictIdBitmap.getLongIterator();
    FieldSpec.DataType storedType = dictionary.getValueType();
    switch (storedType) {
      case INT:
        while (iterator.hasNext()) {
          valueBitmap.add(dictionary.getIntValue((int) iterator.next()));
        }
        break;
      case LONG:
        while (iterator.hasNext()) {
          valueBitmap.add(Long.hashCode(dictionary.getLongValue((int) iterator.next())));
        }
        break;
      case FLOAT:
        while (iterator.hasNext()) {
          valueBitmap.add(Float.hashCode(dictionary.getFloatValue((int) iterator.next())));
        }
        break;
      case DOUBLE:
        while (iterator.hasNext()) {
          valueBitmap.add(Double.hashCode(dictionary.getDoubleValue((int) iterator.next())));
        }
        break;
      case STRING:
        while (iterator.hasNext()) {
          valueBitmap.add(dictionary.getStringValue((int) iterator.next()).hashCode());
        }
        break;
      default:
        throw new IllegalStateException(
          "Illegal data type for DISTINCT_COUNT_BITMAP64 aggregation function: " + storedType);
    }
    return valueBitmap;
  }

  private static final class DictIdsWrapper {
    final Dictionary _dictionary;
    final Roaring64Bitmap _dictIdBitmap;

    private DictIdsWrapper(Dictionary dictionary) {
      _dictionary = dictionary;
      _dictIdBitmap = new Roaring64Bitmap();
    }
  }
}
