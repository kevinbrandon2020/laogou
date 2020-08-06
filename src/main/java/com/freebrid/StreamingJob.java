/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.freebrid;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		CheckpointConfig config = env.getCheckpointConfig();
		config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
		env.setStateBackend(new RocksDBStateBackend("file:///Users/kevinbrandon/Downloads/flinkCheckpoint"));
		DataStreamSource<String> localhost = env.socketTextStream("localhost", 9999);

		SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = localhost.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

			@Override
			public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
				for (String s1 : s.split(" ")) {
					collector.collect(new Tuple2<>(s1, 1));
				}
			}
		});
		SingleOutputStreamOperator<Tuple2<String, Integer>> sum = tuple2SingleOutputStreamOperator.keyBy(0).sum(1);
		sum.print();


		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
