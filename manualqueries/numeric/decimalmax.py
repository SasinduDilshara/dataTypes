# r = 131072
r = 0
r2 = 16383
c = -1
s = ''
while True:
    c+=1
    if(c==r):
        break
    s+='9'

c= 0 
s+='.'
while True:
    c+=1
    if(c==r2):
        break
    s+='9'


f = open("decimalmin.txt", "a")
f.write(s)
f.close()