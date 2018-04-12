import pytest
import time
import stream

def test_timing():
    s = stream.Stream(data=[{'topic':'a', 'messages': ['one', 'two', 'three']},
                     {'topic':'2', 'messages':['a','b','c']}], time_scale=100)
    time.sleep(0.1)
    print(s)
    print('there')
    print(s.get_message('a', 0) )
    assert s.get_message('a', 0) == 'one'
    assert s.get_message('a', 1) == 'two'
    assert s.get_message('2', 2) == 'c'
    assert s.get_message('a', 2) == 'three'


def test_offset():
    s = stream.Stream(data=[{'topic':'a', 'messages': ['one', 'two', 'three']},
                     {'topic':'2', 'messages':['a','b','c']}], time_scale=100)
    time.sleep(0.1)
    assert s.read_bytes('a', 5, 2) == 'wo'
