/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.EndOfChannelStateEvent;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.watermarkstatus.StatusWatermarkValve;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Base class for network-based StreamTaskInput where each channel has a designated {@link
 * RecordDeserializer} for spanning records. Specific implementation bind it to a specific {@link
 * RecordDeserializer}.
 */
public abstract class AbstractStreamTaskNetworkInput<
                T, R extends RecordDeserializer<DeserializationDelegate<StreamElement>>>
        implements StreamTaskInput<T> {


    protected final CheckpointedInputGate checkpointedInputGate;
    protected final DeserializationDelegate<StreamElement> deserializationDelegate;
    protected final TypeSerializer<T> inputSerializer;

    // 这个可以看出来 每个InputChannelInfo 对应一个反序列化器
    protected final Map<InputChannelInfo, R> recordDeserializers;
    // 被展开的ChannelInfo目录
    protected final Map<InputChannelInfo, Integer> flattenedChannelIndices = new HashMap<>();

    /** Valve that controls how watermarks and watermark statuses are forwarded. */
    protected final StatusWatermarkValve statusWatermarkValve;

    protected final int inputIndex;
    private InputChannelInfo lastChannel = null;
    private R currentRecordDeserializer = null;

    public AbstractStreamTaskNetworkInput(
            CheckpointedInputGate checkpointedInputGate,
            TypeSerializer<T> inputSerializer,
            StatusWatermarkValve statusWatermarkValve,
            int inputIndex,
            Map<InputChannelInfo, R> recordDeserializers) {
        super();
        // 接受一个Gate
        this.checkpointedInputGate = checkpointedInputGate;
        // 一个反序列化器
        deserializationDelegate = new NonReusingDeserializationDelegate<>(new StreamElementSerializer<>(inputSerializer));

        this.inputSerializer = inputSerializer;

        for (InputChannelInfo i : checkpointedInputGate.getChannelInfos()) {
            // 顺序记录IC的位置
            flattenedChannelIndices.put(i, flattenedChannelIndices.size());
        }

        this.statusWatermarkValve = checkNotNull(statusWatermarkValve);
        this.inputIndex = inputIndex;
        this.recordDeserializers = checkNotNull(recordDeserializers);
    }

    @Override
    public DataInputStatus emitNext(DataOutput<T> output) throws Exception {

        while (true) {
            // 第二次进入函数 反序列化器 currentRecordDeserializer 不为空
            if (currentRecordDeserializer != null) {

                // 反序列化结果标签 DeserializationResult
                RecordDeserializer.DeserializationResult result;

                try {
                    // 随着 不断有 buffer被 放入 currentRecordDeserializer中，然后借助
                    // deserializationDelegate 将 buffer 转为 StreamElement
                    result = currentRecordDeserializer.getNextRecord(deserializationDelegate);
                } catch (IOException e) {
                    throw new IOException(
                            String.format("Can't get next record for channel %s", lastChannel), e);
                }
                if (result.isBufferConsumed()) {
                    currentRecordDeserializer = null;
                }

                if (result.isFullRecord()) {
                    /**
                     * =============== 代码边界 =================
                     *     processElement(deserializationDelegate.getInstance(), output);
                     *     是处理 StreamElement
                     *     注意：这个函数被执行完，那么这条结果已经被下发到RP
                     * =============== 代码边界 =================
                     */
                    processElement(deserializationDelegate.getInstance(), output);
                    return DataInputStatus.MORE_AVAILABLE;
                }
            }
            // 第一次进入函数 反序列化器 currentRecordDeserializer 是空的
            /**
             * =============== 代码边界 =================
             *     checkpointedInputGate.pollNext() 是从网络中读取数据
             *     注意 这条数据并没有被当前的StreamTask处理！
             * =============== 代码边界 =================
             */
            Optional<BufferOrEvent> bufferOrEvent = checkpointedInputGate.pollNext();
            // 需要从 checkpointedInputGate 中读取出 bufferOrEvent
            if (bufferOrEvent.isPresent()) {
                // return to the mailbox after receiving a checkpoint barrier to avoid processing of
                // data after the barrier before checkpoint is performed for unaligned checkpoint
                // mode
                // 如果 bufferOrEvent 是 buffer，那么就把 buffer 放到 currentRecordDeserializer 中
                if (bufferOrEvent.get().isBuffer()) {
                    processBuffer(bufferOrEvent.get());
                } else {
                // 如果 bufferOrEvent 是 event，那么就直接返回处理状态吧
                    return processEvent(bufferOrEvent.get());
                }
            } else {
                // 如果 bufferOrEvent 是 空的，说明有两种可能：
                // 1.inputGate已经finish了
                // ----> 直接返回 END_OF_INPUT
                // 2.inputGate没有读出来数据
                // ----> 直接返回 NOTHING_AVAILABLE
                if (checkpointedInputGate.isFinished()) {
                    checkState(
                            checkpointedInputGate.getAvailableFuture().isDone(),
                            "Finished BarrierHandler should be available");
                    return DataInputStatus.END_OF_INPUT;
                }
                return DataInputStatus.NOTHING_AVAILABLE;
            }
        }
    }

    private void processElement(StreamElement recordOrMark, DataOutput<T> output) throws Exception {
        /**
         * 目前获取到了 StreamElement 以及 DataOutput
         * 应当借助 DataOutput 完成 StreamElement 的输出
         */
        if (recordOrMark.isRecord()) {
            // 如果是Record
            // 那么就调用 output.emitRecord
            // 在这里是真正的把数据发送给DataOutput
            // 但是在DataOutput这个层级并没有真正接触到RW..
            // 只是发送给了 OperatorChain的头节点 这一点可以再深入看一下，点进去吧
            output.emitRecord(recordOrMark.asRecord());
        } else if (recordOrMark.isWatermark()) {
            // 如果是Watermark
            // 那么就调用 statusWatermarkValve.inputWatermark
            statusWatermarkValve.inputWatermark(
                    recordOrMark.asWatermark(), flattenedChannelIndices.get(lastChannel), output);
        } else if (recordOrMark.isLatencyMarker()) {
            // 如果是LatencyMarker
            // 那么就调用 output.emitLatencyMarker
            output.emitLatencyMarker(recordOrMark.asLatencyMarker());
        } else if (recordOrMark.isWatermarkStatus()) {
            // 如果是WatermarkStatus
            // 那么就调用 statusWatermarkValve.inputWatermarkStatus
            statusWatermarkValve.inputWatermarkStatus(
                    recordOrMark.asWatermarkStatus(),
                    flattenedChannelIndices.get(lastChannel),
                    output);
        } else {
            throw new UnsupportedOperationException("Unknown type of StreamElement");
        }
    }

    protected DataInputStatus processEvent(BufferOrEvent bufferOrEvent) {
        // Event received
        final AbstractEvent event = bufferOrEvent.getEvent();
        if (event.getClass() == EndOfData.class) {
            switch (checkpointedInputGate.hasReceivedEndOfData()) {
                case NOT_END_OF_DATA:
                    // skip
                    break;
                case DRAINED:
                    return DataInputStatus.END_OF_DATA;
                case STOPPED:
                    return DataInputStatus.STOPPED;
            }
        } else if (event.getClass() == EndOfPartitionEvent.class) {
            // release the record deserializer immediately,
            // which is very valuable in case of bounded stream
            releaseDeserializer(bufferOrEvent.getChannelInfo());
            if (checkpointedInputGate.isFinished()) {
                return DataInputStatus.END_OF_INPUT;
            }
        } else if (event.getClass() == EndOfChannelStateEvent.class) {
            if (checkpointedInputGate.allChannelsRecovered()) {
                return DataInputStatus.END_OF_RECOVERY;
            }
        }
        return DataInputStatus.MORE_AVAILABLE;
    }

    protected void processBuffer(BufferOrEvent bufferOrEvent) throws IOException {
        lastChannel = bufferOrEvent.getChannelInfo();
        checkState(lastChannel != null);
        currentRecordDeserializer = getActiveSerializer(bufferOrEvent.getChannelInfo());
        checkState(
                currentRecordDeserializer != null,
                "currentRecordDeserializer has already been released");

        currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
    }

    protected R getActiveSerializer(InputChannelInfo channelInfo) {
        return recordDeserializers.get(channelInfo);
    }

    @Override
    public int getInputIndex() {
        return inputIndex;
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        if (currentRecordDeserializer != null) {
            return AVAILABLE;
        }
        return checkpointedInputGate.getAvailableFuture();
    }

    @Override
    public void close() throws IOException {
        // release the deserializers . this part should not ever fail
        for (InputChannelInfo channelInfo : new ArrayList<>(recordDeserializers.keySet())) {
            releaseDeserializer(channelInfo);
        }
    }

    protected void releaseDeserializer(InputChannelInfo channelInfo) {
        R deserializer = recordDeserializers.get(channelInfo);
        if (deserializer != null) {
            // recycle buffers and clear the deserializer.
            deserializer.clear();
            recordDeserializers.remove(channelInfo);
        }
    }
}
