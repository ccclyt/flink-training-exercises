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
import com.dataartisans.flinktraining.exercises.datastream_java.sources.CheckpointedTaxiFareSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Java reference implementation for the "Hourly Tips" exercise of the Flink training
 * (http://training.data-artisans.com).
 *
 * The task of the exercise is to first calculate the total tips collected by each driver, hour by hour, and
 * then from that stream, find the highest tip total in each hour.
 *
 * Parameters:
 * -input path-to-input-file
 *
 */
public class HourlyTips {

	public static void main(String[] args) throws Exception {

		// read parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		String input = params.getRequired("input");

		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// start the data generator
		DataStream<TaxiFare> fares = env.addSource(
				new CheckpointedTaxiFareSource(input, servingSpeedFactor));

		// compute tips per hour for each driver
		DataStream<Tuple3<Long, Long, Float>> hourlyTips =
		fares.keyBy(t -> t.driverId)
				.timeWindow(Time.hours(1))
                .aggregate(new AggregateFunction<TaxiFare, Tuple2<Long, Float>, Tuple2<Long, Float>>() {
					@Override
					public Tuple2<Long, Float> createAccumulator() {
						return new Tuple2<>(0L, 0f);
					}

					@Override
					public Tuple2<Long, Float> add(TaxiFare taxiFare, Tuple2<Long, Float> longFloatTuple2) {
						return new Tuple2<>(taxiFare.driverId, taxiFare.tip + longFloatTuple2.f1);
					}

					@Override
					public Tuple2<Long, Float> getResult(Tuple2<Long, Float> longFloatTuple2) {
						return longFloatTuple2;
					}

					@Override
					public Tuple2<Long, Float> merge(Tuple2<Long, Float> longFloatTuple2, Tuple2<Long, Float> acc1) {
						return new Tuple2<>(longFloatTuple2.f0,longFloatTuple2.f1+acc1.f1);
					}
				}, new ProcessWindowFunction<Tuple2<Long, Float>, Tuple3<Long, Long, Float>, Long, TimeWindow>() {

					@Override
					public void process(Long aLong, Context context, Iterable<Tuple2<Long, Float>> iterable, Collector<Tuple3<Long, Long, Float>> collector) throws Exception {
						Tuple2<Long, Float> next = iterable.iterator().next();
						collector.collect(new Tuple3<>(context.window().getEnd(),next.f0,next.f1));
					}
				})
				;

		// find the highest total tips in each hour
		DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlyTips
                .keyBy(0)
				.timeWindow(Time.hours(1))
				.maxBy(2);

		// print the result on stdout
		hourlyMax.print();

		// execute the transformation pipeline
		env.execute("Hourly Tips (java)");
	}

	/**
	 * Adds up the tips.
	 */
	public static class AddTips implements WindowFunction<
			TaxiFare, // input type
			Tuple3<Long, Long, Float>, // output type
			Long, // key type
			TimeWindow> // window type
	{

		@Override
		public void apply(
				Long key,
				TimeWindow window,
				Iterable<TaxiFare> fares,
				Collector<Tuple3<Long, Long, Float>> out) throws Exception {

			float sumOfTips = 0;
			for(TaxiFare fare : fares) {
				sumOfTips += fare.tip;
			}

			out.collect(new Tuple3<>(window.getEnd(), key, sumOfTips));
		}
	}
}
