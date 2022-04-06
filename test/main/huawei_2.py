
def min_num(arr1,arr2):
    arr1.sort()
    arr2.sort()
    print(arr1)
    print(arr2)
    n1=arr1[0]+arr1[1]+arr2[0]*2
    n2=arr1[0]*2+arr2[0]+arr2[1]
    if n1>=n2:
        return n2
    else:
        return n1



if __name__ == '__main__':
    arr2=[3,1,1,3]
    arr1=[3,1,2,3]

    n=min_num(arr1,arr2)
    print(n)