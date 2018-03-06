package com.quancheng.saluki.example.server;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.quancheng.saluki.example.repository.StudentDao;
import com.quancheng.saluki.example.repository.entity.StudentDo;

@RestController
public class ScProviderController {

  @Autowired
  private StudentDao studentDao;

  @Transactional
  @RequestMapping("/normal")
  public String normal() {
    StudentDo studentDo = new StudentDo();
    studentDo.setName("liushiming");
    studentDao.save(studentDo);
    return "ok";
  }

  @RequestMapping("/exception")
  public String exception() {
    throw new RuntimeException("rollback");
  }


  @RequestMapping("/providerhealth")
  public String health() {
    return "UP";
  }


}
