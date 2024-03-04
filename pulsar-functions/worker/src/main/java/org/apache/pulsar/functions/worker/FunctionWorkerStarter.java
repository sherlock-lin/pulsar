/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.functions.worker;

import static org.apache.commons.lang3.StringUtils.isBlank;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.ShutdownUtil;
import org.apache.pulsar.docs.tools.CmdGenerateDocs;

/**
 * A starter to start function worker.
 */
@Slf4j
public class FunctionWorkerStarter {

    private static class WorkerArguments {
        @Parameter(
            names = { "-c", "--conf" },
            description = "Configuration File for Function Worker")
        private String configFile;

        @Parameter(names = {"-h", "--help"}, description = "Show this help message")
        private boolean help = false;

        @Parameter(names = {"-g", "--generate-docs"}, description = "Generate docs")
        private boolean generateDocs = false;
    }

    public static void main(String[] args) throws Exception {
        //Worker参数的对象类
        WorkerArguments workerArguments = new WorkerArguments();
        //专门负责解析指令参数的，这里会将args中的参数解析成WorkerArguments对象，方便后面处理
        JCommander commander = new JCommander(workerArguments);
        commander.setProgramName("FunctionWorkerStarter");

        // 参数转换
        commander.parse(args);

        //如果指令中包含help，则在控制台打印相关信息
        if (workerArguments.help) {
            commander.usage();
            return;
        }

        //生成worker doc文档
        if (workerArguments.generateDocs) {
            CmdGenerateDocs cmd = new CmdGenerateDocs("pulsar");
            cmd.addCommand("functions-worker", workerArguments);
            cmd.run(null);
            return;
        }

        //Worker的配置类，如果没有通过-c指定配置，则默认读取functions_worker.yml内容
        WorkerConfig workerConfig;
        if (isBlank(workerArguments.configFile)) {
            workerConfig = new WorkerConfig();
        } else {
            //进行配置加载，将yml文件内容加载成Java对象，方便后续读取
            workerConfig = WorkerConfig.load(workerArguments.configFile);
        }

        final Worker worker = new Worker(workerConfig);
        try {
            //启动Worker服务的入口
            worker.start();
        } catch (Throwable th) {
            log.error("Encountered error in function worker.", th);
            worker.stop();
            ShutdownUtil.triggerImmediateForcefulShutdown();
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Stopping function worker service...");
            worker.stop();
        }));
    }
}
