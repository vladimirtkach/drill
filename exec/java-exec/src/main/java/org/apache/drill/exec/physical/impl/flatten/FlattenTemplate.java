/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.physical.impl.flatten;

import javax.inject.Named;

import io.netty.buffer.DrillBuf;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.exception.OversizedAllocationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.holders.IntervalHolder;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.RecordBatch;

import org.apache.drill.exec.util.Text;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.DateVector;
import org.apache.drill.exec.vector.Float4Vector;
import org.apache.drill.exec.vector.Float8Vector;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.IntervalVector;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableBitVector;
import org.apache.drill.exec.vector.NullableDateVector;
import org.apache.drill.exec.vector.NullableFloat4Vector;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.NullableIntervalVector;
import org.apache.drill.exec.vector.NullableTimeStampVector;
import org.apache.drill.exec.vector.NullableTimeVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.RepeatedBigIntVector;
import org.apache.drill.exec.vector.RepeatedBitVector;
import org.apache.drill.exec.vector.RepeatedVarCharVector;
import org.apache.drill.exec.vector.TimeStampVector;
import org.apache.drill.exec.vector.TimeVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.exec.vector.complex.ListVector;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.RepeatedListVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.exec.vector.complex.writer.BigIntWriter;
import org.apache.drill.exec.vector.complex.writer.BitWriter;
import org.apache.drill.exec.vector.complex.writer.VarCharWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class FlattenTemplate implements Flattener {
  private static final Logger logger = LoggerFactory.getLogger(FlattenTemplate.class);

  private static final int OUTPUT_ROW_COUNT = ValueVector.MAX_ROW_COUNT;

  private BufferAllocator outputAllocator;
  private SelectionVectorMode svMode;
  private RepeatedValueVector fieldToFlatten;
  private ValueVector flattenVector;
  private RepeatedValueVector.RepeatedAccessor accessor;
  private int valueIndex;

  /**
   * The output batch limit starts at OUTPUT_ROW_COUNT, but may be decreased
   * if records are found to be large.
   */
  private int outputLimit = OUTPUT_ROW_COUNT;

  // this allows for groups to be written between batches if we run out of space, for cases where we have finished
  // a batch on the boundary it will be set to 0
  private int innerValueIndex = -1;
  private int currentInnerValueIndex;
  private FragmentContext context;

  @Override
  public void setFlattenField(RepeatedValueVector flattenField) {
    this.fieldToFlatten = flattenField;
    this.accessor = RepeatedValueVector.RepeatedAccessor.class.cast(flattenField.getAccessor());
  }

  @Override
  public RepeatedValueVector getFlattenField() {
    return fieldToFlatten;
  }

  @Override
  public void setOutputCount(int outputCount) {
    outputLimit = outputCount;
  }

  @Override
  public final int flattenRecords(final int recordCount, final int firstOutputIndex,
      final Flattener.Monitor monitor) {
    switch (svMode) {
      case FOUR_BYTE:
        throw new UnsupportedOperationException("Flatten does not support selection vector inputs.");

      case TWO_BYTE:
        throw new UnsupportedOperationException("Flatten does not support selection vector inputs.");

      case NONE:
        if (innerValueIndex == -1) {
          innerValueIndex = 0;
        }

        final int initialInnerValueIndex = currentInnerValueIndex;
        int valueIndexLocal = valueIndex;
        int innerValueIndexLocal = innerValueIndex;
        int currentInnerValueIndexLocal = currentInnerValueIndex;
        outer: {
          int outputIndex = firstOutputIndex;
          int recordsThisCall = 0;
          final int valueCount = accessor.getValueCount();
          int flattenIndex = 0;
          FlattenListHandler flattenListHandler = new FlattenListHandler();
          for ( ; valueIndexLocal < valueCount; valueIndexLocal++) {
            // at least one iteration should be done, to "write null value" and not skip other fields
            final int innerValueCount = accessor.getInnerValueCountAt(valueIndexLocal) == 0 ? 1 : accessor.getInnerValueCountAt(valueIndexLocal);
            for ( ; innerValueIndexLocal < innerValueCount; innerValueIndexLocal++) {
              // If we've hit the batch size limit, stop and flush what we've got so far.
              if (recordsThisCall == outputLimit) {
                // Flush this batch.
                break outer;
              }

              try {
                if (accessor.getInnerValueCountAt(valueIndexLocal) != 0){
                  handleValue(currentInnerValueIndexLocal, flattenIndex, flattenListHandler);
                }

                flattenIndex = accessor.getInnerValueCountAt(valueIndexLocal) == 0 ? flattenIndex : flattenIndex + 1;
                doEval(valueIndexLocal, outputIndex);

              } catch (OversizedAllocationException ex) {
                // unable to flatten due to a soft buffer overflow. split the batch here and resume execution.
                logger.debug("Reached allocation limit. Splitting the batch at input index: {} - inner index: {} - current completed index: {}",
                    valueIndexLocal, innerValueIndexLocal, currentInnerValueIndexLocal);

                /*
                 * TODO
                 * We can't further reduce the output limits here because it won't have
                 * any effect. The vectors have already gotten large, and there's currently
                 * no way to reduce their size. Ideally, we could reduce the outputLimit,
                 * and reduce the size of the currently used vectors.
                 */
                break outer;
              } catch (SchemaChangeException e) {
                throw new UnsupportedOperationException(e);
              }
              outputIndex++;
              currentInnerValueIndexLocal++;
              ++recordsThisCall;
            }
            innerValueIndexLocal = 0;
          }
        }
        // save state to heap
        valueIndex = valueIndexLocal;
        innerValueIndex = innerValueIndexLocal;
        currentInnerValueIndex = currentInnerValueIndexLocal;
        // transfer the computed range
        final int delta = currentInnerValueIndexLocal - initialInnerValueIndex;
        return delta;
      default:
        throw new UnsupportedOperationException();
    }
  }

  private void handleValue(int currentInnerValueIndexLocal, int flattenIndex, FlattenListHandler flattenListHandler) {
    switch (fieldToFlatten.getField().getType().getMinorType()) {
      case BIGINT:
        ((NullableBigIntVector) flattenVector).getMutator().setSafe(currentInnerValueIndexLocal,
                ((BigIntVector)fieldToFlatten.getDataVector()).getAccessor().getObject(flattenIndex));
        break;

      case INT:
        ((NullableIntVector) flattenVector).getMutator().setSafe(currentInnerValueIndexLocal,
                ((IntVector)fieldToFlatten.getDataVector()).getAccessor().getObject(flattenIndex));
        break;

      case FLOAT4:
        ((NullableFloat4Vector) flattenVector).getMutator().setSafe(currentInnerValueIndexLocal,
                ((Float4Vector)fieldToFlatten.getDataVector()).getAccessor().getObject(flattenIndex));
        break;

      case FLOAT8:
        ((NullableFloat8Vector) flattenVector).getMutator().setSafe(currentInnerValueIndexLocal,
                ((Float8Vector)fieldToFlatten.getDataVector()).getAccessor().getObject(flattenIndex));
        break;

      case BIT:
        ((NullableBitVector) flattenVector).getMutator().setSafe(currentInnerValueIndexLocal,
                ((BitVector)fieldToFlatten.getDataVector()).getAccessor().get(flattenIndex));
        break;

      case VARCHAR:
        byte[] varcharValue = ((VarCharVector)fieldToFlatten.getDataVector()).getAccessor().get(flattenIndex);
        ((NullableVarCharVector) flattenVector).getMutator().setSafe(currentInnerValueIndexLocal,
                varcharValue, 0, varcharValue.length);
        break;

      case DATE:
        long dateValue = ((DateVector)fieldToFlatten.getDataVector()).getAccessor().get(flattenIndex);
        ((NullableDateVector) flattenVector).getMutator().setSafe(currentInnerValueIndexLocal,dateValue);
        break;

      case TIME:
        int timeValue = ((TimeVector)fieldToFlatten.getDataVector()).getAccessor().get(flattenIndex);
        ((NullableTimeVector) flattenVector).getMutator().setSafe(currentInnerValueIndexLocal,timeValue);
        break;

      case TIMESTAMP:
        long timestampValue = ((TimeStampVector)fieldToFlatten.getDataVector()).getAccessor().get(flattenIndex);
        ((NullableTimeStampVector) flattenVector).getMutator().setSafe(currentInnerValueIndexLocal,timestampValue);
        break;

      case INTERVAL:
        IntervalHolder holder = new IntervalHolder();
        ((IntervalVector) fieldToFlatten.getDataVector()).getAccessor().get(flattenIndex, holder);
        ((NullableIntervalVector) flattenVector).getMutator()
                .setSafe(currentInnerValueIndexLocal, holder);
        break;

      case MAP:
        ((MapVector) flattenVector).copyFromSafe(flattenIndex, currentInnerValueIndexLocal,(RepeatedMapVector) fieldToFlatten);
        break;

      case LIST:
        flattenListHandler.writeIndex = currentInnerValueIndexLocal;
        flattenListHandler.readIndex = flattenIndex;

        if(fieldToFlatten.getField().getDataMode() == TypeProtos.DataMode.OPTIONAL) {
          //for the case of  flatten(flatten(col)) query
          flattenListHandler.handleOptionalList();
        } else {
          ListVector listVector = ((ListVector) flattenVector);
          flattenListHandler.listPositionSet = false;
          flattenListHandler.handleRepeatedList(fieldToFlatten, listVector.getWriter());
        }

        break;
      default:
    }
  }

  private class FlattenListHandler {
    int writeIndex,readIndex;
    boolean listPositionSet;

    void handleRepeatedList(ValueVector vector, BaseWriter.ListWriter listWriter) {
      TypeProtos.MinorType type = vector.getField().getChildren()
              .iterator().next().getType().getMinorType();
      if(!listPositionSet) { //checking if this is first iteration in recursion
        listPositionSet = true;
        listWriter.setPosition(writeIndex);
      }
      listWriter.startList();
      switch (type) {
        //nested lists handling
        case LIST:
          RepeatedValueVector fromListVector = (RepeatedValueVector) ((RepeatedValueVector) vector).getDataVector();
          for (Object val : (List)fromListVector.getAccessor().getObject(readIndex)) {
            handleRepeatedList(fromListVector, listWriter.list());
          }
          break;

      case VARCHAR:
          RepeatedVarCharVector fromVarcharVector = (RepeatedVarCharVector)  ((RepeatedListVector)vector).getDataVector();
          VarCharWriter varcharWriter = listWriter.varChar();
          for (Text v : fromVarcharVector.getAccessor().getObject(readIndex)) {
            DrillBuf buff = context.getManagedBuffer(v.getLength());
            buff.writeBytes(v.getBytes());
            varcharWriter.writeVarChar(0, v.getLength(), buff);
          }
          break;

      case BIGINT:
        RepeatedBigIntVector fromBigIntVector = (RepeatedBigIntVector)  ((RepeatedListVector)vector).getDataVector();
        BigIntWriter bigIntWriter = listWriter.bigInt();
        for (Long v : fromBigIntVector.getAccessor().getObject(readIndex)) {
          bigIntWriter.writeBigInt(v);
        }
        break;


      case BIT:
        RepeatedBitVector fromBitVector = (RepeatedBitVector)  ((RepeatedListVector)vector).getDataVector();
        BitWriter bitWriter = listWriter.bit();
        for (Boolean v : fromBitVector.getAccessor().getObject(readIndex)) {
          bitWriter.writeBit(v ? 1 : 0);
        }
        break;
      }
      listWriter.endList();
    }

    void handleOptionalList() {
      // other cases in progress now
      Long val = ((NullableBigIntVector)fieldToFlatten.getDataVector()).getAccessor().getObject(readIndex);
      ((NullableBigIntVector) flattenVector).getMutator().setSafe(writeIndex, val);
    }
  }

  @Override
  public final void setup(FragmentContext context, RecordBatch incoming, RecordBatch outgoing,  ValueVector flattenVector)  throws SchemaChangeException{

    this.svMode = incoming.getSchema().getSelectionVectorMode();
    switch (svMode) {
      case FOUR_BYTE:
        throw new UnsupportedOperationException("Flatten does not support selection vector inputs.");
      case TWO_BYTE:
        throw new UnsupportedOperationException("Flatten does not support selection vector inputs.");
    }
    this.flattenVector = flattenVector;
    outputAllocator = outgoing.getOutgoingContainer().getAllocator();
    this.context = context;
    doSetup(context, incoming, outgoing);
  }

  @Override
  public void resetGroupIndex() {
    this.valueIndex = 0;
    this.currentInnerValueIndex = 0;
  }

  public abstract void doSetup(@Named("context") FragmentContext context,
                               @Named("incoming") RecordBatch incoming,
                               @Named("outgoing") RecordBatch outgoing) throws SchemaChangeException;
  public abstract boolean doEval(@Named("inIndex") int inIndex,
                                 @Named("outIndex") int outIndex) throws SchemaChangeException;
}
