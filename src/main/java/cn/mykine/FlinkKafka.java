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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.Kafka011TableSource;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.List;
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

        //分流操作split，给流数据打标签
        SplitStream<SensorReading> splitStream =
                dataStream2.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading value) {
                return value.getTemperature() > 30 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        //合流操作connect-只能合并两条流，数据类型可以不同
        DataStream<Tuple2<String,Double>> highWarnDataStream =
                splitStream.select("high")
                .map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(SensorReading value) throws Exception {
                       return new Tuple2<>(value.getId(),value.getTemperature());
                    }
                });
        //两个不同类型的流合并到一个流中
        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedDataStream =
                highWarnDataStream.connect(splitStream.select("low"));
        //对合并流中的各个流数据提炼成同一种数据类型，方便后续操作
        DataStream<Object> resultConnectDataStream =
                connectedDataStream.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> value1) throws Exception {
                return new Tuple3<>(value1.f0, value1.f1, "高温报警");
            }

            @Override
            public Object map2(SensorReading value2) throws Exception {
                return new Tuple2<>(value2.getId(), "温度正常");
            }
        });

        //sink-输出结果
        splitStream.select("high").print("high");
        splitStream.select("low").print("low");
        splitStream.select("high","low").print("all");
        resultConnectDataStream.print("合流后处理的数据");

//        resultFlatMap.print("flatMap");
//        resultMap.print("map");
//        temperatureMax.print("temperatureMax");
//        reduceResult.print("reduceResult");
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
