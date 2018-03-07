/*
 * Copyright 2014-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.quancheng.springcloud.example;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;

import io.dts.client.aop.DtsTransactionScaner;
import io.dts.client.aop.annotation.EnableDtsConfiguration;

/**
 * @author liushiming
 * @version SpringCloudExampleClientApp.java, v 0.0.1 2018年3月6日 下午3:37:08 liushiming
 */
@SpringBootApplication
@EnableDtsConfiguration
@EnableDiscoveryClient
public class ScExampleClientApp implements CommandLineRunner {
  @Autowired
  private DtsTransactionScaner scaner;

  public static void main(String[] args) {
    SpringApplication.run(ScExampleClientApp.class, args);
  }

  @Override
  public void run(String... arg0) throws Exception {
    System.out.println(scaner);
  }

  @Bean
  public RestTemplate restTemplate() {
    return new RestTemplate();
  }


}
