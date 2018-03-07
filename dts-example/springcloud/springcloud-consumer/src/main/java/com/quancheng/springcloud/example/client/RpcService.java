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
package com.quancheng.springcloud.example.client;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.loadbalancer.LoadBalancerClient;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import io.dts.client.aop.annotation.DtsTransaction;

/**
 * @author liushiming
 * @version RpcService.java, v 0.0.1 2017年11月7日 下午1:38:13 liushiming
 */
@Service
public class RpcService {

  @Autowired
  LoadBalancerClient loadBalancerClient;

  @Autowired
  RestTemplate restTemplate;

  @DtsTransaction
  public String callService() {
    String normalServiceUrl = this.typeServiceUrl("normal");
    String exceptionServiceUrl = this.typeServiceUrl("exception");
    // call normalService
    restTemplate.getForObject(normalServiceUrl, String.class);
    // call excepitonService
    restTemplate.getForObject(exceptionServiceUrl, String.class);
    return "ok";
  }

  private String typeServiceUrl(String type) {
    ServiceInstance serviceInstance = loadBalancerClient.choose("springcloud-provider");
    String url =
        "http://" + serviceInstance.getHost() + ":" + serviceInstance.getPort() + "/" + type;
    return url;
  }



}
