package com.example.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by WeiYang on 2019/1/11.
 *
 * @Author: WeiYang
 * @Package com.example.utils
 * @Project: dmp_24
 * @Title:
 * @Description: Please fill description of the file here
 * @Date: 2019/1/11 8:52
 */
public class MobileUtils {


    public static void main(String[] args){
        String mo = "513513";
        System.out.println(isFixedPhone(mo) || isPhone(mo));
        System.out.println(isFixedPhone(mo) );
        System.out.println( isPhone(mo));

        String mo2 = "d513513";
        System.out.println(isFixedPhone(mo2) || isPhone(mo2));
    }



    /**
     * 校验移动电话（手机）号码合法性
     *
     * @param phone
     * @return
     */
    public static boolean isPhone(String phone) {
        String regex = "^((13[0-9])|(14[5,7,9])|(15([0-3]|[5-9]))|(166)|(17[0,1,3,5,6,7,8])|(18[0-9])|(19[8|9]))\\d{8}$";
        if (phone.length() != 11) {
            return false;
        } else {
            Pattern p = Pattern.compile(regex);
            Matcher m = p.matcher(phone);
            boolean isMatch = m.matches();
            return isMatch;
        }
    }


    /**
     * 校验固定电话号码合法性
     *
     * @param str
     * @return
     */
    public static boolean isFixedPhone(String str) {
        Pattern p1, p2;
        Matcher m;
        boolean isPhone;
        String regex1 = "^[0][1-9]{2,3}-[0-9]{5,10}$";
        String regex2 = "^[1-9]{1}[0-9]{5,8}$";
        // 验证带区号的
        p1 = Pattern.compile(regex1);
        // 验证没有区号的
        p2 = Pattern.compile(regex2);
        if (str.length() > 9) {
            m = p1.matcher(str);
            isPhone = m.matches();
        } else {
            m = p2.matcher(str);
            isPhone = m.matches();
        }
        return isPhone;
    }


}
