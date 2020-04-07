package com.fashion.kafka.message;

/**
 * message实体类
 */
public class MessageInfo {

    private String value;

    private String customerId;

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }
}
