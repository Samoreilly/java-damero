package net.damero;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TestEvent {
    private String id;
    private String message;
    private boolean shouldFail;
}