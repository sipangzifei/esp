#coding=utf-8

class Fox:
    counter = 0
    def __init__(self, _name, _age):
        self.name = _name
        self.age  = _age
        Fox.counter += 1
    def cry(self, _word):
        print('%s -- %s -- %d' % (_word, self.name, self.age))

if __name__ == '__main__':
    print('xx')
    f = Fox('nick', 12)
    f.cry('miao')
    print('zz')

