package com.mapr.fuse.entity;

@SuppressWarnings("WeakerAccess")
public class MessageRange {
    private Integer topicOffset;
    private Integer messageOffset;
    private Integer messageSize;

    @java.beans.ConstructorProperties({"topicOffset", "messageOffset", "messageSize"})
    public MessageRange(Integer topicOffset, Integer messageOffset, Integer messageSize) {
        this.topicOffset = topicOffset;
        this.messageOffset = messageOffset;
        this.messageSize = messageSize;
    }

    /**
     * @return offset from the start of message
     */
    public Integer getOffsetFromStartMessage() {
        int offsetFromStart = 0;
        if (messageOffset != 0) {
            offsetFromStart = messageSize - messageOffset;
        }
        return offsetFromStart;
    }

    public Integer getTopicOffset() {
        return this.topicOffset;
    }

    public Integer getMessageOffset() {
        return this.messageOffset;
    }

    public Integer getMessageSize() {
        return this.messageSize;
    }

    public void setTopicOffset(Integer topicOffset) {
        this.topicOffset = topicOffset;
    }

    public void setMessageOffset(Integer messageOffset) {
        this.messageOffset = messageOffset;
    }

    public void setMessageSize(Integer messageSize) {
        this.messageSize = messageSize;
    }

    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof MessageRange)) return false;
        final MessageRange other = (MessageRange) o;
        if (!other.canEqual(this)) return false;
        final Object this$topicOffset = this.getTopicOffset();
        final Object other$topicOffset = other.getTopicOffset();
        if (this$topicOffset == null ? other$topicOffset != null : !this$topicOffset.equals(other$topicOffset))
            return false;
        final Object this$messageOffset = this.getMessageOffset();
        final Object other$messageOffset = other.getMessageOffset();
        if (this$messageOffset == null ? other$messageOffset != null : !this$messageOffset.equals(other$messageOffset))
            return false;
        final Object this$messageSize = this.getMessageSize();
        final Object other$messageSize = other.getMessageSize();
        return this$messageSize == null ? other$messageSize == null : this$messageSize.equals(other$messageSize);
    }

    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final Object $topicOffset = this.getTopicOffset();
        result = result * PRIME + ($topicOffset == null ? 43 : $topicOffset.hashCode());
        final Object $messageOffset = this.getMessageOffset();
        result = result * PRIME + ($messageOffset == null ? 43 : $messageOffset.hashCode());
        final Object $messageSize = this.getMessageSize();
        result = result * PRIME + ($messageSize == null ? 43 : $messageSize.hashCode());
        return result;
    }

    protected boolean canEqual(Object other) {
        return other instanceof MessageRange;
    }

    public String toString() {
        return "com.mapr.fuse.entity.MessageRange(topicOffset=" + this.getTopicOffset() + ", messageOffset=" + this.getMessageOffset() + ", messageSize=" + this.getMessageSize() + ")";
    }
}
