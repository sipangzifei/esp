# -*- coding: UTF-8 -*-


from sailog  import *
from saiconf import *
from saiutil import *
from saidb   import *

 
name = "lzl"
 
def f1():
    print(name)
 
def f2():
    name = "eric"
    f1()
 
f2()
