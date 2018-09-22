package com.mapr.fuse.entity;

@SuppressWarnings("WeakerAccess")
public class TopicRange {
    private MessageRange startOffset;
    private MessageRange endOffset;

    @java.beans.ConstructorProperties({"startOffset", "endOffset"})
    public TopicRange(MessageRange startOffset, MessageRange endOffset) {
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    /**
     * @return number of messages in the range
     */
    public Integer getNumberOfMessages() {
        int numberOfMessages = endOffset.getTopicOffset() - startOffset.getTopicOffset();
        if (endOffset.getMessageOffset() > 0) {
            numberOfMessages++;
        }
        return numberOfMessages;
    }

    public MessageRange getStartOffset() {
        return this.startOffset;
    }

    public MessageRange getEndOffset() {
        return this.endOffset;
    }

    public void setStartOffset(MessageRange startOffset) {
        this.startOffset = startOffset;
    }

    public void setEndOffset(MessageRange endOffset) {
        this.endOffset = endOffset;
    }

    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof TopicRange)) return false;
        final TopicRange other = (TopicRange) o;
        if (!other.canEqual(this)) return false;
        final Object this$startOffset = this.getStartOffset();
        final Object other$startOffset = other.getStartOffset();
        if (this$startOffset == null ? other$startOffset != null : !this$startOffset.equals(other$startOffset))
            return false;
        final Object this$endOffset = this.getEndOffset();
        final Object other$endOffset = other.getEndOffset();
        return this$endOffset == null ? other$endOffset == null : this$endOffset.equals(other$endOffset);
    }

    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final Object $startOffset = this.getStartOffset();
        result = result * PRIME + ($startOffset == null ? 43 : $startOffset.hashCode());
        final Object $endOffset = this.getEndOffset();
        result = result * PRIME + ($endOffset == null ? 43 : $endOffset.hashCode());
        return result;
    }

    protected boolean canEqual(Object other) {
        return other instanceof TopicRange;
    }

    public String toString() {
        return "com.mapr.fuse.entity.TopicRange(startOffset=" + this.getStartOffset() + ", endOffset=" + this.getEndOffset() + ")";
    }
}
