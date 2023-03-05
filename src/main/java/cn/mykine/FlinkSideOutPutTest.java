package cn.mykine;

import cn.mykine.apitest.bean.SensorReading;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Collections;
import java.util.Properties;

public class FlinkSideOutPutTest {

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
        DataStream<SensorReading> dataStream = receiveStreamData.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] valueArr = value.split(",");
                return new SensorReading(valueArr[0],
                        Long.valueOf(valueArr[1]),
                        Double.valueOf(valueArr[2]));
            }
        });

        //分流操作-side output操作，给流数据打标签
        OutputTag<SensorReading> highTempOutput = new OutputTag<SensorReading>("highTemp"){};
        OutputTag<SensorReading> lowTempOutput = new OutputTag<SensorReading>("lowTemp"){};
        SingleOutputStreamOperator<SensorReading> processStream = dataStream.process(
            new ProcessFunction<SensorReading, SensorReading>() {
                @Override
                public void processElement(SensorReading value, ProcessFunction<SensorReading, SensorReading>.Context ctx, Collector<SensorReading> out) throws Exception {
                    if(value.getTemperature()>=30){
                        //高温数据输出到高温tag的侧输出流
                        ctx.output(highTempOutput,value);
                    }else if(value.getTemperature()<10){
                        ctx.output(lowTempOutput,value);
                    }else{
                        out.collect(value);
                    }
                }
            }
        );

        //sink-输出结果
        processStream.getSideOutput(highTempOutput).print("highTemp");
        processStream.getSideOutput(lowTempOutput).print("lowTemp");
        processStream.print("normal");

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
