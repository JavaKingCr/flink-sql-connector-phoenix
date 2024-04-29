package org.apache.flink.connector.phoenix;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.LocalDateTime;

public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        DataStream<Row> userStream = env
                .fromElements(
                        Row.of(LocalDateTime.parse("2021-08-21T13:00:00"), 1, "Alice"),
                        Row.of(LocalDateTime.parse("2021-08-21T13:05:00"), 2, "Bob"),
                        Row.of(LocalDateTime.parse("2021-08-21T13:10:00"), 2, "NewYork"))
                .returns(
                        Types.ROW_NAMED(
                                new String[]{"ts", "uid", "name"},
                                Types.LOCAL_DATE_TIME, Types.INT, Types.STRING));

        tableEnv.createTemporaryView(
                "UserTable",
                userStream,
                Schema.newBuilder()
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .column("uid", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .columnByExpression("proc_time", "PROCTIME()")
//                        .watermark("ts", "ts - INTERVAL '1' SECOND")
                        .build());


        TableResult tableResult = tableEnv.executeSql("\n" +
                "CREATE TABLE us_population (\n" +
                "      state VARCHAR NOT NULL,\n" +
                "      city VARCHAR NOT NULL,\n" +
                "      population BIGINT,\n" +
                "      PRIMARY KEY (state, city) NOT ENFORCED \n" +
                "      ) WITH (\n" +
                "  'connector.type' = 'phoenix', \n" +
                "  'connector.url' = 'jdbc:phoenix:10:2181', \n" +
                "  'connector.table' = 'US_POPULATION',  \n" +
                "  'connector.driver' = 'org.apache.phoenix.jdbc.PhoenixDriver', \n" +
                "  'connector.username' = '', \n" +
                "  'connector.password' = '',\n" +
                "  'phoenix.schema.isnamespacemappingenabled' = 'true',\n" +
                "  'phoenix.schema.mapsystemtablestonamespace' = 'true',\n" +
                "  'connector.write.flush.max-rows' = '1'\n" +
                " )\n");

//        tableEnv.executeSql("select * from us_population").print();

        tableEnv.executeSql("SELECT name, uid,b.population FROM UserTable as a " +
                "left join us_population  FOR SYSTEM_TIME AS OF a.proc_time  as b on a.name=b.city").print();

    }
}
