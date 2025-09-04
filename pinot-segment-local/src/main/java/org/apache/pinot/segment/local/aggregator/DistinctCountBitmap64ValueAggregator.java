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
package org.apache.pinot.segment.local.aggregator;

import org.apache.pinot.common.utils.Roaring64BitmapUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.longlong.Roaring64Bitmap;


public class DistinctCountBitmap64ValueAggregator implements ValueAggregator<Object, Roaring64Bitmap> {
  public static final DataType AGGREGATED_VALUE_TYPE = DataType.BYTES;

  private int _maxByteSize;

  @Override
  public AggregationFunctionType getAggregationType() {
    return AggregationFunctionType.DISTINCTCOUNTBITMAP64;
  }

  @Override
  public DataType getAggregatedValueType() {
    return AGGREGATED_VALUE_TYPE;
  }

  @Override
  public Roaring64Bitmap getInitialAggregatedValue(Object rawValue) {
    Roaring64Bitmap initialValue;
    if (rawValue instanceof byte[]) {
      byte[] bytes = (byte[]) rawValue;
      initialValue = deserializeAggregatedValue(bytes);
      _maxByteSize = Math.max(_maxByteSize, bytes.length);
    } else {
      initialValue = new Roaring64Bitmap();
      initialValue.add(rawValue.hashCode());
      _maxByteSize = Math.max(_maxByteSize, (int) initialValue.serializedSizeInBytes());
    }
    return initialValue;
  }

  @Override
  public Roaring64Bitmap applyRawValue(Roaring64Bitmap value, Object rawValue) {
    if (rawValue instanceof byte[]) {
      value.or(deserializeAggregatedValue((byte[]) rawValue));
    } else {
      value.add(rawValue.hashCode());
    }
    _maxByteSize = Math.max(_maxByteSize, (int) value.serializedSizeInBytes());
    return value;
  }

  @Override
  public Roaring64Bitmap applyAggregatedValue(Roaring64Bitmap value, Roaring64Bitmap aggregatedValue) {
    value.or(aggregatedValue);
    _maxByteSize = Math.max(_maxByteSize, (int) value.serializedSizeInBytes());
    return value;
  }

  @Override
  public Roaring64Bitmap cloneAggregatedValue(Roaring64Bitmap value) {
    return value.clone();
  }

  @Override
  public boolean isAggregatedValueFixedSize() {
    return false;
  }

  @Override
  public int getMaxAggregatedValueByteSize() {
    return _maxByteSize;
  }

  @Override
  public byte[] serializeAggregatedValue(Roaring64Bitmap value) {
    return Roaring64BitmapUtils.serialize(value);
  }

  @Override
  public Roaring64Bitmap deserializeAggregatedValue(byte[] bytes) {
    return Roaring64BitmapUtils.deserialize(bytes);
  }
}
