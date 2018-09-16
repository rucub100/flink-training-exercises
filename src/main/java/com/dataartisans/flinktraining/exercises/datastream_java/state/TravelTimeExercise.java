package com.dataartisans.flinktraining.exercises.datastream_java.state;

import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.CheckpointedTaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TravelTimePredictionModel;

public class TravelTimeExercise extends ExerciseBase {
	public static void main(String[] args) throws Exception {
		
		ParameterTool params = ParameterTool.fromArgs(args);
		final String ridesFile = params.get("rides", pathToRideData);
		
		final int servingSpeedFactor = 1800; 	// 30 minutes worth of events are served every second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);
		env.enableCheckpointing(1000);
		env.setStateBackend((StateBackend)new FsStateBackend("file:///tmp/checkpoints"));
		env.setRestartStrategy(
				  RestartStrategies.fixedDelayRestart(
				    60,                            // 60 retries
				    Time.of(10, TimeUnit.SECONDS)  // 10 secs delay
				  ));

		DataStream<TaxiRide> rides = env
				.addSource(rideSourceOrTest(new CheckpointedTaxiRideSource(ridesFile, servingSpeedFactor)));

		// Tuple (ride id, predicted time in minutes)
		DataStream<Tuple2<Long, Integer>> result = rides
				.filter(new NYCFilter())
				.map(new DestinationGridMap())
				.keyBy(new CellIdKeySelector())
				.flatMap(new PredictionModelMap());
		
		printOrTest(result);
		
		env.execute();
	}
	
	public static class PredictionModelMap extends RichFlatMapFunction<Tuple2<Integer, TaxiRide>, Tuple2<Long, Integer>> {
		
		private ValueState<TravelTimePredictionModel> predictionState;
		
		@Override
		public void open(Configuration parameters) throws Exception {
			predictionState = getRuntimeContext().getState(
					new ValueStateDescriptor<TravelTimePredictionModel>(
							"prediction", 
							TravelTimePredictionModel.class));
		}
		
		@Override
		public void flatMap(Tuple2<Integer, TaxiRide> value, Collector<Tuple2<Long, Integer>> out) throws Exception {
			TravelTimePredictionModel predictionModel = predictionState.value();
			
			if (predictionModel == null) {
				predictionModel = new TravelTimePredictionModel();
			}
			
		    if (value.f1.isStart) {
		    	// predict
		    	int prediction = predictionModel.predictTravelTime(
		    			GeoUtils.getDirectionAngle(
		    					value.f1.startLon, 
		    					value.f1.startLat, 
		    					value.f1.endLon, 
		    					value.f1.endLat), 
		    			GeoUtils.getEuclideanDistance(
		    					value.f1.startLon, 
		    					value.f1.startLat, 
		    					value.f1.endLon, 
		    					value.f1.endLat));
		    	
		    	out.collect(new Tuple2<Long, Integer>(value.f1.rideId, prediction));
		    } else {
		    	// refine
		    	int direction = GeoUtils.getDirectionAngle(
    					value.f1.startLon, 
    					value.f1.startLat, 
    					value.f1.endLon, 
    					value.f1.endLat);
		    	double distance = GeoUtils.getEuclideanDistance(
    					value.f1.startLon, 
    					value.f1.startLat, 
    					value.f1.endLon, 
    					value.f1.endLat);
		    	double travelTime = calcTravelTime(value.f1);
		    	predictionModel.refineModel(direction, distance, travelTime);
		    	predictionState.update(predictionModel);
		    }
		}
	}
	
	public static class CellIdKeySelector implements KeySelector<Tuple2<Integer, TaxiRide>, Integer> {
		@Override
		public Integer getKey(Tuple2<Integer, TaxiRide> tuple) throws Exception {
			return tuple.f0;
		}
	}
	
	public static class DestinationGridMap implements MapFunction<TaxiRide, Tuple2<Integer, TaxiRide>> {
		@Override
		public Tuple2<Integer, TaxiRide> map(TaxiRide ride) throws Exception {
			return new Tuple2(GeoUtils.mapToGridCell(ride.endLon, ride.endLat), ride);
		}
	}
	
	public static class NYCFilter implements FilterFunction<TaxiRide> {
		@Override
		public boolean filter(TaxiRide ride) throws Exception {
			return GeoUtils.isInNYC(ride.startLon, ride.startLat) &&
					GeoUtils.isInNYC(ride.endLon, ride.endLat);
		}
	}

	public static double calcTravelTime(TaxiRide ride) {
		return (ride.endTime.getMillis() - ride.startTime.getMillis()) / 1000.0 / 60.0;
	}
}
