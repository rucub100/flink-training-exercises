/*
 * Copyright 2018 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flinktraining.exercises.datastream_java.windows;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.MissingSolutionException;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction.Context;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;

/**
 * The "Hourly Tips" exercise of the Flink training
 * (http://training.data-artisans.com).
 *
 * The task of the exercise is to first calculate the total tips collected by each driver, hour by hour, and
 * then from that stream, find the highest tip total in each hour.
 *
 * Parameters:
 * -input path-to-input-file
 *
 */
public class HourlyTipsExercise extends ExerciseBase {

	public static void main(String[] args) throws Exception {

		// read parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", ExerciseBase.pathToFareData);

		final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		// start the data generator
		DataStream<TaxiFare> fares = env.addSource(fareSourceOrTest(new TaxiFareSource(input, maxEventDelay, servingSpeedFactor)));
		
		DataStream<Tuple3<Long, Long, Float>> hourlyMax = fares
				// keyBy driver id
				.map(new MapFunction<TaxiFare, Tuple2<Long, Float>>() {

					@Override
					public Tuple2<Long, Float> map(TaxiFare value) throws Exception {
						return new Tuple2<>(value.driverId, value.tip);
					}
				})
				.keyBy((Tuple2<Long, Float> t) -> t.f0)
				.timeWindow(Time.hours(1))
				.aggregate(new AddTips(), new WrapWithWindowInfo())
				.timeWindowAll(Time.hours(1))
				.maxBy(2);

		printOrTest(hourlyMax);

		// execute the transformation pipeline
		env.execute("Hourly Tips (java)");
	}

	/**
	 * Adds up the tips.
	 */
	public static class AddTips implements AggregateFunction<
			Tuple2<Long, Float>, // input type
			Float,    // accumulator type
			Float     // output type
		>

	{
		@Override
		public Float createAccumulator() {
			return 0F;
		}

		@Override
		public Float add(Tuple2<Long, Float> fare, Float aFloat) {
			return fare.f1 + aFloat;
		}

		@Override
		public Float getResult(Float aFloat) {
			return aFloat;
		}

		@Override
		public Float merge(Float aFloat, Float accumulator) {
			return aFloat + accumulator;
		}
	}

	/*
	 * Wraps the pre-aggregated result into a tuple along with the window's timestamp and key.
	 */
	public static class WrapWithWindowInfo extends ProcessWindowFunction<
			Float, Tuple3<Long, Long, Float>, Long, TimeWindow> {
		@Override
		public void process(Long key, Context context, Iterable<Float> elements, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
			Float sumOfTips = elements.iterator().next();
			out.collect(new Tuple3<>(context.window().getEnd(), key, sumOfTips));
		}
	}
	
}
