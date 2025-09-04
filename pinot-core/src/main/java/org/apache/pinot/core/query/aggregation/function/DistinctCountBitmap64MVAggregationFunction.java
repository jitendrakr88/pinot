/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.query.aggregation.function;

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.longlong.Roaring64Bitmap;


/**
 * The {@code DistinctCountBitmap64MVAggregationFunction} calculates the number of distinct values for a given multi-value
 * expression using Roaring64Bitmap. The bitmap stores the actual values for {@code INT} expression, or hash code of the
 * values for other data types (values with the same hash code will only be counted once).
 */
public class DistinctCountBitmap64MVAggregationFunction extends DistinctCountBitmap64AggregationFunction {

  public DistinctCountBitmap64MVAggregationFunction(List<ExpressionContext> arguments) {
    super(verifySingleArgument(arguments, "DISTINCT_COUNT_BITMAP64_MV"));
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTBITMAP64MV;
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      Roaring64Bitmap dictIdBitmap = getDictIdBitmap(aggregationResultHolder, dictionary);
      int[][] dictIds = blockValSet.getDictionaryIdsMV();
      for (int i = 0; i < length; i++) {
        for (int dictId : dictIds[i]) {
          dictIdBitmap.add(dictId);
        }
      }
      return;
    }

    // For non-dictionary-encoded expression, store hash code of the values into the bitmap
    Roaring64Bitmap valueBitmap = getValueBitmap(aggregationResultHolder);
    DataType storedType = blockValSet.getValueType().getStoredType();
    switch (storedType) {
      case INT:
        int[][] intValues = blockValSet.getIntValuesMV();
        for (int i = 0; i < length; i++) {
          for (int value : intValues[i]) {
            valueBitmap.add(value);
          }
        }
        break;
      case LONG:
        long[][] longValues = blockValSet.getLongValuesMV();
        for (int i = 0; i < length; i++) {
          for (long value : longValues[i]) {
            valueBitmap.add(Long.hashCode(value));
          }
        }
        break;
      case FLOAT:
        float[][] floatValues = blockValSet.getFloatValuesMV();
        for (int i = 0; i < length; i++) {
          for (float value : floatValues[i]) {
            valueBitmap.add(Float.hashCode(value));
          }
        }
        break;
      case DOUBLE:
        double[][] doubleValues = blockValSet.getDoubleValuesMV();
        for (int i = 0; i < length; i++) {
          for (double value : doubleValues[i]) {
            valueBitmap.add(Double.hashCode(value));
          }
        }
        break;
      case STRING:
        String[][] stringValues = blockValSet.getStringValuesMV();
        for (int i = 0; i < length; i++) {
          for (String value : stringValues[i]) {
            valueBitmap.add(value.hashCode());
          }
        }
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_BITMAP64_MV aggregation function: " + storedType);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[][] dictIds = blockValSet.getDictionaryIdsMV();
      for (int i = 0; i < length; i++) {
        Roaring64Bitmap bitmap = getDictIdBitmap(groupByResultHolder, groupKeyArray[i], dictionary);
        for (int dictId : dictIds[i]) {
          bitmap.add(dictId);
        }
      }
      return;
    }

    // For non-dictionary-encoded expression, store hash code of the values into the bitmap
    DataType storedType = blockValSet.getValueType().getStoredType();
    switch (storedType) {
      case INT:
        int[][] intValues = blockValSet.getIntValuesMV();
        for (int i = 0; i < length; i++) {
          Roaring64Bitmap bitmap = getValueBitmap(groupByResultHolder, groupKeyArray[i]);
          for (int value : intValues[i]) {
            bitmap.add(value);
          }
        }
        break;
      case LONG:
        long[][] longValues = blockValSet.getLongValuesMV();
        for (int i = 0; i < length; i++) {
          Roaring64Bitmap bitmap = getValueBitmap(groupByResultHolder, groupKeyArray[i]);
          for (long value : longValues[i]) {
            bitmap.add(Long.hashCode(value));
          }
        }
        break;
      case FLOAT:
        float[][] floatValues = blockValSet.getFloatValuesMV();
        for (int i = 0; i < length; i++) {
          Roaring64Bitmap bitmap = getValueBitmap(groupByResultHolder, groupKeyArray[i]);
          for (float value : floatValues[i]) {
            bitmap.add(Float.hashCode(value));
          }
        }
        break;
      case DOUBLE:
        double[][] doubleValues = blockValSet.getDoubleValuesMV();
        for (int i = 0; i < length; i++) {
          Roaring64Bitmap bitmap = getValueBitmap(groupByResultHolder, groupKeyArray[i]);
          for (double value : doubleValues[i]) {
            bitmap.add(Double.hashCode(value));
          }
        }
        break;
      case STRING:
        String[][] stringValues = blockValSet.getStringValuesMV();
        for (int i = 0; i < length; i++) {
          Roaring64Bitmap bitmap = getValueBitmap(groupByResultHolder, groupKeyArray[i]);
          for (String value : stringValues[i]) {
            bitmap.add(value.hashCode());
          }
        }
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_BITMAP64_MV aggregation function: " + storedType);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[][] dictIds = blockValSet.getDictionaryIdsMV();
      for (int i = 0; i < length; i++) {
        for (int groupKey : groupKeysArray[i]) {
          Roaring64Bitmap bitmap = getDictIdBitmap(groupByResultHolder, groupKey, dictionary);
          for (int dictId : dictIds[i]) {
            bitmap.add(dictId);
          }
        }
      }
      return;
    }

    // For non-dictionary-encoded expression, store hash code of the values into the bitmap
    DataType storedType = blockValSet.getValueType().getStoredType();
    switch (storedType) {
      case INT:
        int[][] intValues = blockValSet.getIntValuesMV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            Roaring64Bitmap bitmap = getValueBitmap(groupByResultHolder, groupKey);
            for (int value : intValues[i]) {
              bitmap.add(value);
            }
          }
        }
        break;
      case LONG:
        long[][] longValues = blockValSet.getLongValuesMV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            Roaring64Bitmap bitmap = getValueBitmap(groupByResultHolder, groupKey);
            for (long value : longValues[i]) {
              bitmap.add(Long.hashCode(value));
            }
          }
        }
        break;
      case FLOAT:
        float[][] floatValues = blockValSet.getFloatValuesMV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            Roaring64Bitmap bitmap = getValueBitmap(groupByResultHolder, groupKey);
            for (float value : floatValues[i]) {
              bitmap.add(Float.hashCode(value));
            }
          }
        }
        break;
      case DOUBLE:
        double[][] doubleValues = blockValSet.getDoubleValuesMV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            Roaring64Bitmap bitmap = getValueBitmap(groupByResultHolder, groupKey);
            for (double value : doubleValues[i]) {
              bitmap.add(Double.hashCode(value));
            }
          }
        }
        break;
      case STRING:
        String[][] stringValues = blockValSet.getStringValuesMV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            Roaring64Bitmap bitmap = getValueBitmap(groupByResultHolder, groupKey);
            for (String value : stringValues[i]) {
              bitmap.add(value.hashCode());
            }
          }
        }
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_BITMAP64_MV aggregation function: " + storedType);
    }
  }
} 