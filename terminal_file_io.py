from sys import argv

script, filename = argv
a_file = open(filename)
print a_file.read()