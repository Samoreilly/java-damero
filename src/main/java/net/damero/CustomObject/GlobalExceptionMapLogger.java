package net.damero.CustomObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GlobalExceptionMapLogger {
    public static Map<Object, List<Throwable>> exceptions = new HashMap<>();

    public static int getExceptionSize(Object object){
        List<Throwable> list = exceptions.get(object);
        return list == null ? 0 : list.size();
    }
    public static void addObject(Object object, Throwable throwable){
        exceptions.computeIfAbsent(object, k -> new ArrayList<>()).add(throwable);
    }
}
