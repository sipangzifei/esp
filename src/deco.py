# -*- coding: UTF-8 -*-


from sailog  import *
from saiconf import *
from saiutil import *
from saidb   import *


########################################################################
def simple_decorator(f):
    def wrapper():
        print '1'
        f()
        print '2'

    return wrapper

@simple_decorator
def hello1():
    print "Hello World1"

########################################################################


def decorator_factory(enter_message, exit_message):
    # We're going to return this decorator
    def simple_decorator(f):
        def wrapper():
            print enter_message
            f()
            print exit_message
 
        return wrapper
 
    return simple_decorator
 
@decorator_factory("Start", "End")
def hello2():
    print "Hello World2"

########################################################################

 
hello1()
hello2()


