

if __name__ == '__main__':
    #矢量化运算实验
    a=[i for i in range(50)]
    b=[i+1 for i in range(50)]
    c=[0 for i in range(len(a))]
    # for i in range(len(a)):
    #     c[i]=a[i]+b[i]
    for i in range(0,len(a),4):
        c[i]=a[i]+b[i]
        c[i+1]=a[i+1]+b[i+1]
        c[i+2]=a[i+2]+b[i+2]
        c[i+3]=a[i+3]+b[i+3]
    # print(b)
    print(c)
    print(len(c))