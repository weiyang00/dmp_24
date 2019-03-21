package com.example.utils;

/**
 * Created by WeiYang on 2019/1/22.
 *
 * @Author: WeiYang
 * @Package com.example.utils
 * @Project: dmp_24
 * @Title:
 * @Description: Please fill description of the file here
 * @Date: 2019/1/22 8:44
 */
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringNum {

    public static void main(String[] args) {
        String a="电话：86-0539-5839182";
       String out = StringNum.getNum(a);
        System.out.println( out);
    }


    public static String getNum(String input){
        String regEx="[^0-9|-]";
        Pattern p = Pattern.compile(regEx);
        Matcher m = p.matcher(input);
        return m.replaceAll("").trim();
    }

}