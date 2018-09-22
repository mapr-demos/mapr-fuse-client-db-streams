package com.mapr.fuse.dto;

public class MessageConfig {
    private String start = "";
    private String stop = "";
    private String separator = "";
    private Boolean size = false;

    @java.beans.ConstructorProperties({"start", "stop", "separator", "size"})
    public MessageConfig(String start, String stop, String separator, Boolean size) {
        this.start = start;
        this.stop = stop;
        this.separator = separator;
        this.size = size;
    }

    public MessageConfig() {
    }

    public String getStart() {
        return this.start;
    }

    public String getStop() {
        return this.stop;
    }

    public String getSeparator() {
        return this.separator;
    }

    public Boolean getSize() {
        return this.size;
    }

    public void setStart(String start) {
        this.start = start;
    }

    public void setStop(String stop) {
        this.stop = stop;
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }

    public void setSize(Boolean size) {
        this.size = size;
    }

    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof MessageConfig)) return false;
        final MessageConfig other = (MessageConfig) o;
        if (!other.canEqual(this)) return false;
        final Object this$start = this.getStart();
        final Object other$start = other.getStart();
        if (this$start == null ? other$start != null : !this$start.equals(other$start)) return false;
        final Object this$stop = this.getStop();
        final Object other$stop = other.getStop();
        if (this$stop == null ? other$stop != null : !this$stop.equals(other$stop)) return false;
        final Object this$separator = this.getSeparator();
        final Object other$separator = other.getSeparator();
        if (this$separator == null ? other$separator != null : !this$separator.equals(other$separator)) return false;
        final Object this$size = this.getSize();
        final Object other$size = other.getSize();
        return this$size == null ? other$size == null : this$size.equals(other$size);
    }

    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final Object $start = this.getStart();
        result = result * PRIME + ($start == null ? 43 : $start.hashCode());
        final Object $stop = this.getStop();
        result = result * PRIME + ($stop == null ? 43 : $stop.hashCode());
        final Object $separator = this.getSeparator();
        result = result * PRIME + ($separator == null ? 43 : $separator.hashCode());
        final Object $size = this.getSize();
        result = result * PRIME + ($size == null ? 43 : $size.hashCode());
        return result;
    }

    @SuppressWarnings("WeakerAccess")
    protected boolean canEqual(Object other) {
        return other instanceof MessageConfig;
    }

    public String toString() {
        return "com.mapr.fuse.dto.MessageConfig(start=" + this.getStart() + ", stop=" + this.getStop() + ", separator=" + this.getSeparator() + ", size=" + this.getSize() + ")";
    }
}
