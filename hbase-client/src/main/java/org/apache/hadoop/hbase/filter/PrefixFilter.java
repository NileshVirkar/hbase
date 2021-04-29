/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.filter;

import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ByteStringer;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Pass results that have same row prefix.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class PrefixFilter extends FilterBase {
  protected byte [] prefix = null;
  protected boolean passedPrefix = false;
  protected boolean filterRow = true;

  public PrefixFilter(final byte [] prefix) {
    this.prefix = prefix;
  }

  public byte[] getPrefix() {
    return prefix;
  }

  @Override
  public boolean filterRowKey(byte[] buffer, int offset, int length) {
    if (buffer == null || this.prefix == null)
      return true;
    if (length < prefix.length)
      return true;
    // if they are equal, return false => pass row
    // else return true, filter row
    // if we are passed the prefix, set flag
    int cmp = Bytes.compareTo(buffer, offset, this.prefix.length, this.prefix, 0,
        this.prefix.length);
    if ((!isReversed() && cmp > 0) || (isReversed() && cmp < 0)) {
      passedPrefix = true;
    }
    filterRow = (cmp != 0);
    return filterRow;
  }

  @Override
  public ReturnCode filterKeyValue(Cell v) {
    if (filterRow) return ReturnCode.NEXT_ROW;
    return ReturnCode.INCLUDE;
  }

  // Override here explicitly as the method in super class FilterBase might do a KeyValue recreate.
  // See HBASE-12068
  @Override
  public Cell transformCell(Cell v) {
    return v;
  }

  @Override
  public boolean filterRow() {
    return filterRow;
  }

  @Override
  public void reset() {
    filterRow = true;
  }

  @Override
  public boolean filterAllRemaining() {
    return passedPrefix;
  }

  public static Filter createFilterFromArguments(ArrayList<byte []> filterArguments) {
    Preconditions.checkArgument(filterArguments.size() == 1,
                                "Expected 1 but got: %s", filterArguments.size());
    byte [] prefix = ParseFilter.removeQuotesFromByteArray(filterArguments.get(0));
    return new PrefixFilter(prefix);
  }

  /**
   * @return The filter serialized using pb
   */
  @Override
  public byte [] toByteArray() {
    FilterProtos.PrefixFilter.Builder builder =
      FilterProtos.PrefixFilter.newBuilder();
    if (this.prefix != null) builder.setPrefix(ByteStringer.wrap(this.prefix));
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link PrefixFilter} instance
   * @return An instance of {@link PrefixFilter} made from <code>bytes</code>
   * @throws org.apache.hadoop.hbase.exceptions.DeserializationException
   * @see #toByteArray
   */
  public static PrefixFilter parseFrom(final byte [] pbBytes)
  throws DeserializationException {
    FilterProtos.PrefixFilter proto;
    try {
      proto = FilterProtos.PrefixFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    return new PrefixFilter(proto.hasPrefix()?proto.getPrefix().toByteArray():null);
  }

  /**
   * @param other
   * @return true if and only if the fields of the filter that are serialized
   * are equal to the corresponding fields in other.  Used for testing.
   */
  @Override
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this) return true;
    if (!(o instanceof PrefixFilter)) return false;

    PrefixFilter other = (PrefixFilter)o;
    return Bytes.equals(this.getPrefix(), other.getPrefix());
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + " " + Bytes.toStringBinary(this.prefix);
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Filter && areSerializedFieldsEqual((Filter) obj);
  }

  @Override
  public int hashCode() {
    return Bytes.hashCode(this.getPrefix());
  }
}
