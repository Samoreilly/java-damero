package com.example.kafkaexample.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderEvent {
    private String orderId;
    private String customerId;
    private Double amount;
    private String paymentMethod;
    private String status;
    
    @JsonIgnore
    public String getId() {
        return orderId;
    }
}

