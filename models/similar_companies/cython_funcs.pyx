
def ie_weighted_dist(double[:] ar1, double[:] ar2):
    cdef double summa = 0
    cdef double delta = 0
    cdef int i
    for i in range(10):
        if i == 0: #  city_population
            delta = (20 * (ar1[i] - ar2[i])) ** 2
        elif i == 1: #  revenue_all
            delta = (700 * (ar1[i] - ar2[i])) ** 2
        elif i == 2: #  post_office_latitude
            delta = (30 * (ar1[i] - ar2[i])) ** 2
        elif i == 3: #  post_office_longitude
            delta = (30 * (ar1[i] - ar2[i])) ** 2
        elif i == 4: #  age
            delta = (30 * (ar1[i] - ar2[i])) ** 2
        elif i == 5: #  okved_cnt
            delta = (15 * (ar1[i] - ar2[i])) ** 2
        elif i == 6: #  cnt_comp_in_group
            delta = (20 * (ar1[i] - ar2[i])) ** 2
        elif i == 8: #  okved_code4
            delta = (10 * (ar1[i] - ar2[i])) ** 2
        elif i == 9: #  sex_code
            delta = (10 * (ar1[i] - ar2[i])) ** 2
        else:
            delta = (ar1[i] - ar2[i]) ** 2
        summa += delta
    summa =  summa ** 0.5
    return summa

def comp_weighted_dist(double[:] ar1, double[:] ar2):
    cdef double summa = 0
    cdef double delta = 0
    cdef int i
    for i in range(13):
        if i == 2: #  revenue_all
            delta = (1000 * (ar1[i] - ar2[i])) ** 2
        elif i == 12: #  okved_code4
            delta = (500 * (ar1[i] - ar2[i])) ** 2
        else:
            delta = (ar1[i] - ar2[i]) ** 2
        summa += delta
    summa =  summa ** 0.5
    return summa