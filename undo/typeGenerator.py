num = 12
print("\n\n")
s = ''
for i in range(num):
    inp = input().strip().split(" ")
    a = inp[0]
    a = a.strip()
    s+='"'+a+"Type"+'"'+" : "+'"'+a+'"'+","
    s+="\n"
s = s.strip("\n")
s = s.strip(",")
s = "\n\n" + s

print(s)