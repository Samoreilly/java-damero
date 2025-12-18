package net.damero;

import net.damero.Kafka.Aspect.Components.Utility.EventUnwrapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class EventUnwrapperTest {

    @Test
    public void testShortStringIdPreserved() {
        String input = "short-id-123";
        String id = EventUnwrapper.extractEventId(input, null);
        assertEquals(input, id);
    }

    @Test
    public void testLongStringIdHashed() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1024; i++)
            sb.append('x');
        String longStr = sb.toString();
        String id = EventUnwrapper.extractEventId(longStr, null);
        assertNotNull(id);
        assertTrue(id.startsWith("h:"), "expected hashed id prefix");
        assertFalse(id.contains(longStr), "hashed id should not contain full payload");
    }
}
