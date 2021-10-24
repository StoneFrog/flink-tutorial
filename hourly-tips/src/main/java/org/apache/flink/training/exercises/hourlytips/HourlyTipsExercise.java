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

package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator;
import org.apache.flink.training.exercises.common.utils.ExerciseBase;
import org.apache.flink.training.exercises.common.utils.MissingSolutionException;
import org.apache.flink.util.Collector;

/**
 * The "Hourly Tips" exercise of the Flink training in the docs.
 *
 * <p>The task of the exercise is to first calculate the total tips collected by each driver, hour
 * by hour, and then from that stream, find the highest tip total in each hour.
 */
public class HourlyTipsExercise extends ExerciseBase {

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(ExerciseBase.parallelism);

        // start the data generator
        DataStream<TaxiFare> fares = env.addSource(fareSourceOrTest(new TaxiFareGenerator()));

        DataStream<Tuple3<Long, Long, Float>> hourlyTips =
            fares
                .keyBy((TaxiFare fare) -> fare.driverId)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new TotalTipsAggregator(), new WindowOutputProducer());

        DataStream<Tuple3<Long, Long, Float>> hourlyMax =
                hourlyTips
                        .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                        .maxBy(2);

//        Solution analysis:
//        DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlyTips
//         			.keyBy(t -> t.f0) // key on end_of_window
//         			.maxBy(2); // this doesn't wait for whole key aggregate but outputs new elements as long as there is new maximum
//        more at https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/learn-flink/etl/#aggregations-on-keyed-streams

        printOrTest(hourlyMax);

        // execute the transformation pipeline
        env.execute("Hourly Tips (java)");
    }

    private static class TotalTipsAggregator
            implements AggregateFunction<TaxiFare, Float, Float> {
        @Override
        public Float createAccumulator() {
            return (float) 0;
        }

        @Override
        public Float add(TaxiFare fare, Float accumulator) {
            return accumulator + fare.tip;
        }

        @Override
        public Float getResult(Float accumulator) {
            return accumulator;
        }

        @Override
        public Float merge(Float a, Float b) {
            return a + b;
        }
    }

    private static class WindowOutputProducer
            extends ProcessWindowFunction<Float, Tuple3<Long, Long, Float>, Long, TimeWindow> {
        @Override
        public void process(
                Long key,
                Context context,
                Iterable<Float> totalTips,
                Collector<Tuple3<Long, Long, Float>> out) {
            Float totalTip = totalTips.iterator().next();
            out.collect(new Tuple3<>(context.window().getEnd(), key, totalTip));
        }
    }
}
