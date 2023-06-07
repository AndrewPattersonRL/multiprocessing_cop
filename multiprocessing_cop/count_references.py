import sys

a = []
b = a
print(sys.getrefcount(a))
#
del b
print(sys.getrefcount(a))
