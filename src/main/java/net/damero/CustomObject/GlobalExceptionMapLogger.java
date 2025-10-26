package net.damero.CustomObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GlobalExceptionMapLogger {
    public static Map<EventWrapper, List<Throwable>> exceptions = new HashMap<>();

    public static int getExceptionSize(EventWrapper object){
        List<Throwable> list = exceptions.get(object);
        return list == null ? 0 : list.size();
    }
}
