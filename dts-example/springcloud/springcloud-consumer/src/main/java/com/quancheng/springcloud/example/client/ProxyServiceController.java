package com.quancheng.springcloud.example.client;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/proxy")
public class ProxyServiceController {


  @Autowired
  private RpcService rpcService;

  @RequestMapping("/hello")
  public String hello() {
    return rpcService.callService();
  }

}
