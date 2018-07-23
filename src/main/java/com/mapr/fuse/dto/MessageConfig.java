package com.mapr.fuse.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MessageConfig {
    private String start = "";
    private String stop = "";
    private String separator = "";
    private Boolean size = false;
}
