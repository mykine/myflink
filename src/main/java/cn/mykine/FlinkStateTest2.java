package cn.mykine;

import cn.mykine.apitest.bean.SensorReading;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class FlinkStateTest2 {

    public static void main(String[] args) throws Exception {
        //env-创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //source-从数据源获取流数据
        // kafka 配置项
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.10.135:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> receiveStreamData
                = env.addSource(new FlinkKafkaConsumer011<String>(
                                        "sensor",
                                        new SimpleStringSchema(),
                                        properties
                                    )
                                );

        //transformation-基于流数据进行转换计算
        DataStream<SensorReading> dataStream = receiveStreamData.map(value -> {
            String[] splitData = value.split(",");
            return new SensorReading(splitData[0],Long.valueOf(splitData[1]),Double.valueOf(splitData[2]));
        });
        DataStream<Integer> mapResult = dataStream
                .keyBy("id")
                .map(new MyMap());

        //sink-输出结果
        mapResult.print("mapResult");
        //执行
        env.execute();

    }

    public static class MyMap extends RichMapFunction<SensorReading,Integer>{
        //声明本地变量
        private ValueState<Integer> keyCountState;
//        private MapState<String,Double> myMapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //设置本地变量为key状态
            keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>(
                    "key-count",
                    Integer.class,
                    0
            ));

//            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>(
//                "my-map",
//                String.class,
//                Double.class
//            ));

        }

        @Override
        public Integer map(SensorReading value) throws Exception {
            Integer countCurrent = keyCountState.value();
            countCurrent++;
            keyCountState.update(countCurrent);
            return countCurrent;

//            // map state
//            myMapState.get("1");
//            myMapState.put("2", 12.3);
//            myMapState.remove("2");
//            myMapState.clear();
        }

        @Override
        public void setRuntimeContext(RuntimeContext t) {
            super.setRuntimeContext(t);
        }

        @Override
        public RuntimeContext getRuntimeContext() {
            return super.getRuntimeContext();
        }

        @Override
        public IterationRuntimeContext getIterationRuntimeContext() {
            return super.getIterationRuntimeContext();
        }



        @Override
        public void close() throws Exception {
            super.close();
        }
    }


}
