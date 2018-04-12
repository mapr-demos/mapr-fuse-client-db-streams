#!/usr/bin/env python

import json
import time


class Stream():
    '''Simulates a messsage stream with data stored in a list. Also supports finding the message by byte offset.
    The stream content consists of a list of topics, each of which is a dict with topic and messages. The messages
    element is just a list of strings.'''

    def __init__(self, file=None, data=None, delimiter="\n", time_scale=1):
        if file:
            with open(file) as f:
                self.content = json.load(f)
        else:
            self.content = data
        self.origin = time.time()
        self.delimiter = delimiter
        self.time_scale = time_scale

    def get_messages_for_topic(self, topic):
        data = [x['messages'] for x in self.content if x['topic'] == topic]
        if len(data) == 0:
            return []
        else:
            data = data[0]
            n = self.max_offset(data)
            return data[0:n]

    def get_message(self, topic, offset):
        data = self.get_messages_for_topic(topic)
        if offset > self.max_offset(data):
            return None
        else:
            return data[offset]

    def max_offset(self, messages=None):
        r = int((time.time() - self.origin) * self.time_scale)
        if messages:
            r = min(r, len(messages))
        return r

    def get_messages_from_offset(self, topic, offset):
        if offset < 0:
            offset = 0
        messages = self.get_messages_for_topic(topic)
        c = 0
        i = 0
        while i < len(messages):
            dc = len(messages[i]) + len(self.delimiter)
            if c + dc > offset:
                break
            i = i+1
            c = c + dc

        # end of data
        if i >= len(messages):
            return (i, 0)

        # offset is in message i
        return (i, offset-c)

    def read_bytes(self, topic, offset, count):
        (i0, delta0) = self.get_messages_from_offset(topic, offset)
        (i1, delta1) = self.get_messages_from_offset(topic, offset + count)
        messages = self.get_messages_for_topic(topic)
        r = (messages[i0] + self.delimiter)[delta0:(delta0+count)]
        for i in range(i0 + 1, i1):
            r = r + messages[i] + self.delimiter
        if (i1 > i0) & (i1 < len(messages)):
            r = r + (messages[i1] + self.delimiter)[0:delta1]
        return bytes(r, 'utf-8')

    def size(self, topic):
        delimiter_size = len(self.delimiter)
        return sum([len(m) + delimiter_size for m in self.get_messages_for_topic(topic)])

    def get_topics(self):
        return [str(x['topic']) for x in self.content]
