/*
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

package org.apache.flink.training.exercises.correlatedcalls;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class CorrelatedCallsExercise {

//    private final KafkaSource<CorrelatedCdr> source;
    private final SourceFunction<CorrelatedCdr> source;
    private final SinkFunction sink;
    
    public CorrelatedCallsExercise() {

//        this.source = KafkaSource.<CorrelatedCdr>builder()
//                .setBootstrapServers("192.168.112.163:9093")
//                .setTopics("correlator_correlated_calls")
//                .setGroupId("callsearch_consumer")
//                .setProperty("security.protocol", "SASL_PLAINTEXT")
//                .setProperty("sasl.mechanism", "PLAIN")
//                .setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"callsearch\" password=\"***\";")
//                .setStartingOffsets(OffsetsInitializer.latest())
//                .setValueOnlyDeserializer(new CorrelatedCdrDeserializationSchema())
//                .build();
        this.source = new CorrelatedCdrGenerator();
        this.sink = new PrintSinkFunction<>();
    }
    
    public static void main(String[] args) throws Exception {
        CorrelatedCallsExercise job = new CorrelatedCallsExercise();
        job.execute();
    }
    
    public JobExecutionResult execute() throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        WatermarkStrategy<CorrelatedCdr> cdrWatermarkStrategy = WatermarkStrategy
                .<CorrelatedCdr>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withIdleness(Duration.ofMinutes(1))
                .withTimestampAssigner((cdr, streamRecordTimestamp) -> cdr.getCustomerSbcDisconnectTimeInEpochMilliseconds());
//        DataStreamSource<CorrelatedCdr> cdrs = env.fromSource(source, cdrWatermarkStrategy, "Kafka Source");
        DataStream<CorrelatedCdr> cdrs = env.addSource(source).assignTimestampsAndWatermarks(cdrWatermarkStrategy);

        cdrs
                .filter(cdr -> cdr.getCustomerId() != null && cdr.getSipResponseCode() != null)
                .keyBy(CorrelatedCdr::getCustomerId)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                //.sideOutputLateData() //TODO: output late metrics to datadog or CloudWatch?
//                .aggregate(new CallMetricsAggregate()) // would result in less storage, perhaps slower performance (additional serialization/deserialization)
                .process(new CallMetricsFunction())
                .addSink(sink);

        return env.execute("Correlated CDRs");
    }

    private static class CallMetricsFunction
            extends ProcessWindowFunction<CorrelatedCdr, Tuple3<Long, String, CallMetrics>, String, TimeWindow> {

        @Override
        public void process(
                String key,
                Context context,
                Iterable<CorrelatedCdr> cdrs,
                Collector<Tuple3<Long, String, CallMetrics>> out) {

            long sumOfDuration = 0L;
            long failedCalls = 0L;
            long connectedCalls = 0L;
            for (CorrelatedCdr cdr : cdrs) {
                sumOfDuration += cdr.getCustomerSbcCallDurationInMilliseconds();
                if (cdr.getSipResponseCode() == 200) {
                    connectedCalls++;
                } else {
                    failedCalls++;
                }
            }
            CallMetrics callMetrics = new CallMetrics();
            callMetrics.setCustomerId(key);
            callMetrics.setConnectedCalls(connectedCalls);
            callMetrics.setFailedCalls(failedCalls);
            callMetrics.setMillisecondsOfUse(sumOfDuration);
            out.collect(Tuple3.of(context.window().getEnd(), key, callMetrics));
        }
    }

    private static class CallMetricsAggregate implements AggregateFunction<CorrelatedCdr, CallMetrics, CallMetrics> {
        @Override
        public CallMetrics createAccumulator() {
            return new CallMetrics();
        }

        @Override
        public CallMetrics add(CorrelatedCdr value, CallMetrics accumulator) {
            accumulator.setCustomerId(value.getCustomerId());
            accumulator.addCdr(value);
            return accumulator;
        }

        @Override
        public CallMetrics getResult(CallMetrics accumulator) {
            return accumulator;
        }

        @Override
        public CallMetrics merge(CallMetrics a, CallMetrics b) {
            if (a.getCustomerId().equals(b.getCustomerId())) {
                CallMetrics callMetrics = new CallMetrics();
                callMetrics.setCustomerId(a.getCustomerId());
                callMetrics.setConnectedCalls(a.getConnectedCalls() + b.getConnectedCalls());
                callMetrics.setFailedCalls(a.getFailedCalls() + b.getFailedCalls());
                callMetrics.setMillisecondsOfUse(a.getMillisecondsOfUse() + b.getMillisecondsOfUse());
                return callMetrics;     
            }
            return a;
        }
    }
}
