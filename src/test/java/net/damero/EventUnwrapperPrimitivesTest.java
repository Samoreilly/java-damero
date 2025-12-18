package net.damero;

import net.damero.Kafka.Aspect.Components.Utility.EventUnwrapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class EventUnwrapperPrimitivesTest {

    public static class TestPojo {
        private final String id;

        public TestPojo(String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }
    }

    public static class NoIdPojo {
        private final String name;

        public NoIdPojo(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return "NoId:" + name;
        }
    }

    @Test
    public void shortStringPreserved() {
        assertEquals("abc", EventUnwrapper.extractEventId("abc", null));
    }

    @Test
    public void integerHandled() {
        assertEquals("123", EventUnwrapper.extractEventId(123, null));
    }

    @Test
    public void doubleHandled() {
        assertEquals("3.14", EventUnwrapper.extractEventId(3.14, null));
    }

    @Test
    public void booleanHandled() {
        assertEquals("true", EventUnwrapper.extractEventId(true, null));
    }

    @Test
    public void pojoWithGetId() {
        TestPojo p = new TestPojo("pojo-1");
        assertEquals("pojo-1", EventUnwrapper.extractEventId(p, null));
    }

    @Test
    public void pojoWithoutIdUsesToString() {
        NoIdPojo p = new NoIdPojo("xyz");
        String id = EventUnwrapper.extractEventId(p, null);
        assertTrue(id.contains("NoId:xyz"));
    }
}
