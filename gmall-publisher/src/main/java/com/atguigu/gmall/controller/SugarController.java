package com.atguigu.gmall.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author shkstart
 * @create 2022-03-25 9:52
 */
@RestController
public class SugarController {


    @RequestMapping("/test")
    public String test1(){
        return "success";
    }
    @RequestMapping("/test1")
    public String test2(){
        return "index.html";
    }

}
