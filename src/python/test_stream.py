import pytest
import time
import stream

def test_timing():
    s = stream.Stream(data=[{'topic':'a', 'messages': ['one', 'two', 'three']},
                     {'topic':'2', 'messages':['a','b','c']}], time_scale=100)
    time.sleep(0.1)
    assert s.get_message('a', 0) == 'one'
    assert s.get_message('a', 1) == 'two'
    assert s.get_message('2', 2) == 'c'
    assert s.get_message('a', 2) == 'three'


def test_offset():
    s = stream.Stream(data=[{'topic':'a', 'messages': ['one', 'two', 'three']},
                     {'topic':'2', 'messages':['a','b','c']}], time_scale=100)
    time.sleep(0.1)
    assert s.read_bytes('a', 0, 100) == 'one\ntwo\nthree\n'
    assert s.read_bytes('a', 5, 1) == 'w'
    assert s.read_bytes('a', 5, 2) == 'wo'
    assert s.read_bytes('a', 5, 7) == 'wo\nthre'
    assert s.read_bytes('a', 8, 7) == 'three\n'

def test_topics():
    s = stream.Stream(data=[{'topic':'a', 'messages': ['one', 'two', 'three']},
                            {'topic':'2', 'messages':['a','b','c']}], time_scale=100)
    time.sleep(0.1)
    assert s.get_topics() == ['a', '2']

def test_progress():
    s = stream.Stream(data=[{'topic':'a', 'messages': ['one', 'two', 'three']},
                            {'topic':'2', 'messages':['a','b','c']}], time_scale=5)
    # watch out ... this test is sensitive to delay
    # the idea is to show how we see more and more messages in a topic
    assert s.get_messages_for_topic('a') == []
    assert s.get_messages_for_topic('1') == []
    time.sleep(0.3)
    assert s.get_messages_for_topic('a') == ['one']
    assert s.get_messages_for_topic('2') == ['a']
    time.sleep(0.2)
    assert s.max_offset() == 2
    assert s.get_messages_for_topic('a') == ['one', 'two']
    assert s.get_messages_for_topic('2') == ['a', 'b']

