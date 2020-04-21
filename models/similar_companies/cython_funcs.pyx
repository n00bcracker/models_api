
def weighted_dist(double[:] ar1, double[:] ar2):
    cdef double summa = 0
    cdef double delta = 0
    cdef int i
    for i in range(12):
        if i != 2 :
            delta = (ar1[i] - ar2[i]) ** 2
        else:  # revenue_all
            delta = (1000 * (ar1[i] - ar2[i])) ** 2
        summa += delta
    summa =  summa ** 0.5
    return summa