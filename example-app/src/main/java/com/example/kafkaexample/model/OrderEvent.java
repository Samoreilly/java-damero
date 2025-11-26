package com.example.kafkaexample.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class OrderEvent {
    private String orderId;
    private String customerId;
    private Double amount;
    private String paymentMethod;
    private String status;
    
    public OrderEvent() {}

    public OrderEvent(String orderId, String customerId, Double amount, String paymentMethod, String status) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
        this.paymentMethod = paymentMethod;
        this.status = status;
    }

    @JsonIgnore
    public String getId() {
        return orderId;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public String getPaymentMethod() {
        return paymentMethod;
    }

    public void setPaymentMethod(String paymentMethod) {
        this.paymentMethod = paymentMethod;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}

