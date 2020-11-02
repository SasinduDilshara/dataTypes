a = input()

a = a.split(",")
b = ''
c = ''
for i in range(len(a)):
    a[i] = a[i].strip()
    e = a[i].split()
    e = e[1]
    b+=e+", "
    c+="${"+e+"}, "
    
print(b.strip(", ")+"\n"+c.strip(", "))