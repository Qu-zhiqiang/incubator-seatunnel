/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.example.engine;

import org.apache.seatunnel.core.starter.Seatunnel;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.exception.CommandException;
import org.apache.seatunnel.core.starter.seatunnel.args.ClientCommandArgs;
import org.apache.seatunnel.core.starter.seatunnel.command.ClientCommandBuilder;
import org.apache.seatunnel.engine.common.runtime.ExecutionMode;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

public class SeaTunnelEngineExample {

    public static void main(String[] args) throws FileNotFoundException, URISyntaxException, CommandException {
        System.setProperty("SEATUNNEL_HOME","D:\\work\\seatunnel");
        System.setProperty("HADOOP_USER_NAME","hdfs");
        String configFile = getTestConfigFile("/examples/jdbc.conf");
        ClientCommandArgs clientCommandArgs = new ClientCommandArgs();
        clientCommandArgs.setConfigFile(configFile);
        clientCommandArgs.setCheckConfig(false);
        clientCommandArgs.setJobName("fake_to_console");
        // Change Execution Mode to CLUSTER to use client mode, before do this, you should start SeaTunnelEngineServerExample
        clientCommandArgs.setExecutionMode(ExecutionMode.LOCAL);
        Command<ClientCommandArgs> command =
            new ClientCommandBuilder().buildCommand(clientCommandArgs);
        Seatunnel.run(command);
    }

    public static String getTestConfigFile(String configFile) throws FileNotFoundException, URISyntaxException {
        URL resource = SeaTunnelEngineExample.class.getResource(configFile);
        if (resource == null) {
            throw new FileNotFoundException("Can't find config file: " + configFile);
        }
        return Paths.get(resource.toURI()).toString();
    }

}
