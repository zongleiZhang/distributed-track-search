package com.ada;

import com.ada.flinkFunction.TrackPointTAndW;
import com.ada.geometry.TrackPoint;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamingJob {


    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        env.readTextFile("D:\\研究生资料\\track_data\\成都滴滴\\Sorted_2D\\XY_20161101")
                .map(TrackPoint::new)
                .assignTimestampsAndWatermarks(new TrackPointTAndW())
                .keyBy(new KeySelector<TrackPoint, Integer>() {


                    @Override
                    public Integer getKey(TrackPoint value) throws Exception {
                        return null;
                    }
                })
                ;
    }

}
