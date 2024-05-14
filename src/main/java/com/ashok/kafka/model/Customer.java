package com.ashok.kafka.model;

import lombok.Data;

@Data
public class Customer {

    private Integer id;
    private String name;
    private Long contactNumber;
    private String email;

}
