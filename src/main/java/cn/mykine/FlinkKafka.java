package cn.mykine;

import cn.mykine.apitest.bean.SensorReading;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.Kafka011TableSource;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class FlinkKafka {
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
        SingleOutputStreamOperator<Tuple2<String, String>> resultFlatMap = receiveStreamData
                .flatMap(new MyFlatMap())
                .setParallelism(2);

        SingleOutputStreamOperator<String> resultMap =
                receiveStreamData
                        .filter(new FilterFunction<String>() {
                            @Override
                            public boolean filter(String value) throws Exception {
                                return value.toLowerCase().startsWith("cpu");
                            }
                        })
                        .map(new MyMap()).setParallelism(2);


        DataStream<SensorReading> dataStream = receiveStreamData.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] valueArr = value.split(",");
                return new SensorReading(valueArr[0],
                        Long.valueOf(valueArr[1]),
                        Double.valueOf(valueArr[2]));
            }
        });

        DataStream<SensorReading> dataStream2 = receiveStreamData.map(value->{
            String[] valueArr = value.split(",");
            return new SensorReading(valueArr[0],
                    Long.valueOf(valueArr[1]),
                    Double.valueOf(valueArr[2]));
        });

        KeyedStream<SensorReading, Tuple> keyedStream = dataStream2.keyBy("id");
        DataStream<SensorReading> temperatureMax = keyedStream.maxBy("temperature");
        //reduce算子可以让当前数据与上一次聚合计算的结果进行比较操作，构造出想要的结果数据值
        DataStream<SensorReading> reduceResult = keyedStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading curentMax, SensorReading newValue) throws Exception {
                SensorReading result = new SensorReading(
                        curentMax.getId(),
                        newValue.getTimestamp(),
                        curentMax.getTemperature()>newValue.getTemperature() ? curentMax.getTemperature() : newValue.getTemperature()
                );
                return result;
            }
        });
        //sink-输出结果
//        resultFlatMap.print("flatMap");
//        resultMap.print("map");
        temperatureMax.print("temperatureMax");
        reduceResult.print("reduceResult");
//        dataStream2.print("dataStream2");
        //执行
        env.execute();

    }

    public static class MyMap implements MapFunction<String,String>{

        @Override
        public String map(String value) throws Exception {
            String[] words = value.split(",");
            return words[0]+"的温度是"+words[2];
        }
    }

    public static class MyFlatMap implements FlatMapFunction<String, Tuple2<String,String>>{

        @Override
        public void flatMap(String value, Collector<Tuple2<String, String>> out) throws Exception {
            String[] words = value.split(",");

            out.collect(new Tuple2<>(words[0],words[2]));
        }
    }
}
