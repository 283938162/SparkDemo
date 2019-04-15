package com.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JTest {
    public static void main(String[] args) {
        String a = "b,c";
        List<String>  aList = new ArrayList<>(Arrays.asList(a.split(",")));
        System.out.println(aList);

        aList.add("d");
        System.out.println(aList);
    }
}
