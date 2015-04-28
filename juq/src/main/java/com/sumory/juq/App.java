package com.sumory.juq;

import java.util.Random;

public class App {
    public static void main(String[] args) {
        String s = "a/b/c";
        String[] ss = s.split("/");
        System.out.println(ss[1]);
        for (int i = 0; i < 100; i++)
            System.out.println(new Random(System.currentTimeMillis()).nextInt(10));
    }
}
