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

package org.apache.flink.training.exercises.longrides;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.training.exercises.common.utils.ExerciseBase;
import org.apache.flink.training.exercises.common.utils.MissingSolutionException;
import org.apache.flink.util.Collector;

import java.time.Instant;

/**
 * The "Long Ride Alerts" exercise of the Flink training in the docs.
 *
 * <p>The goal for this exercise is to emit START events for taxi rides that have not been matched
 * by an END event during the first 2 hours of the ride.
 */
public class LongRidesExercise extends ExerciseBase {

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
        DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(new TaxiRideGenerator()));

        DataStream<TaxiRide> longRides =
                rides.keyBy((TaxiRide ride) -> ride.rideId).process(new MatchFunction());

        printOrTest(longRides);

        env.execute("Long Taxi Rides");
    }

    public static class MatchFunction extends KeyedProcessFunction<Long, TaxiRide, TaxiRide> {

        private transient MapState<Long, TaxiRide> taxiRides;

        @Override
        public void open(Configuration config) throws Exception {
            MapStateDescriptor<Long, TaxiRide> taxiRidesDesc =
                    new MapStateDescriptor<>("taxiRides", Long.class, TaxiRide.class);
            taxiRides = getRuntimeContext().getMapState(taxiRidesDesc);
        }

        @Override
        public void processElement(TaxiRide ride, Context context, Collector<TaxiRide> out)
                throws Exception {
            TimerService timerService = context.timerService();
            Long rideId = ride.rideId;
            TaxiRide storedRide = taxiRides.get(rideId);

            if (storedRide == null) {
                // if end ride event comes first and start ride will be lost, key won't be cleaned, we may need ttl.
                // on th other hand we can't fire timer for end event here as it's based on rideStart, so in case
                // rideStart arrives over 2 hours late - end event can be already cleaned up, and we would get false alarm.
                taxiRides.put(rideId, ride);
                if (ride.isStart) {
                    timerService.registerEventTimeTimer(getTimerTime(ride));
                }
            } else {
                if (!ride.isStart) {
                    timerService.deleteEventTimeTimer(getTimerTime(storedRide));
                }
                taxiRides.remove(rideId);
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<TaxiRide> out)
                throws Exception {
            long rideId = context.getCurrentKey();
            TaxiRide storedRide = this.taxiRides.get(rideId);
            // if there was only end ride event - this window is never scheduled
            // if there was end followed by start - this window wouldn't be scheduled as well
            // if there was only start and no end - we have to collect as time has passed
            // if there was start followed by end - end was within given time and canceled window, so it would not be fired
            out.collect(storedRide);
            this.taxiRides.remove(rideId);
        }

        private long getTimerTime(TaxiRide ride) {
            return ride.startTime.plusSeconds(120 * 60).toEpochMilli();
        }
    }
}
