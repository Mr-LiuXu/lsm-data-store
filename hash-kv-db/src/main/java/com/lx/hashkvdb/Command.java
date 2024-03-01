package com.lx.hashkvdb;

public class Command {
    String op;
    String key;
    String value;

    public Command() {
        
    }

    public Command(String op, String key, String value) {
        this.op = op;
        this.key = key;
        this.value = value;
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
