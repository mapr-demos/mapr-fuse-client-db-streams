package com.mapr.fuse;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class CoordinatesDto {
    private Integer startOffset;
    private Integer endOffset;
    private Integer numberOfMessages;
}
