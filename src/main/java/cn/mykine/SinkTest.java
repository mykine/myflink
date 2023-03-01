package cn.mykine;

import cn.mykine.apitest.bean.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

public class SinkTest {

    public static void main(String[] args) throws Exception {
        //env-创建执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment()
                        .setParallelism(3);

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
        DataStream<String> dataStream = receiveStreamData
                .map(new MyRichMap())
                .map(value->{
                    return value.toString();
                });

        //sink-输出结果
        dataStream.addSink(new FlinkKafkaProducer011<String>(
                    "192.168.10.135:9092",
                    "sinkTest",
                    new SimpleStringSchema()
        ));

        //执行
        env.execute();

    }

    public static class MyRichMap extends RichMapFunction<String,SensorReading> {

        @Override
        public SensorReading map(String value) throws Exception {
            String[] valueArr = value.split(",");
            return new SensorReading(valueArr[0],
                    Long.valueOf(valueArr[1]),
                    Double.valueOf(valueArr[2]));
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化工作，比如建立数据库的连接，初始化状态等
            System.out.println(getRuntimeContext().getIndexOfThisSubtask()+":open~~~~~");
        }

        @Override
        public void close() throws Exception {
            //关闭工作，比如关闭数据库连接，清空状态等
            System.out.println(getRuntimeContext().getIndexOfThisSubtask()+":close!!!");
        }
    }

}
