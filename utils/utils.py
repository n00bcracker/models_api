def check_inn(inn):  # проверка корректности ИНН по контрольным суммам
    inn = str(inn)
    digits = [int(x) for x in inn]
    if len(digits) == 12:  # проверка для ИП и ФЛ
        coeff1 = [7, 2, 4, 10, 3, 5, 9, 4, 6, 8]
        checksum1 = sum([x * digits[i] for i, x in enumerate(coeff1)]) % 11 % 10
        coeff2 = [3, 7, 2, 4, 10, 3, 5, 9, 4, 6, 8]
        checksum2 = sum([x * digits[i] for i, x in enumerate(coeff2)]) % 11 % 10

        if checksum1 == digits[-2] and checksum2 == digits[-1]:
            return True
        else:
            return False
    elif len(digits) == 10:  # проверка для ЮЛ
        coeff = [2, 4, 10, 3, 5, 9, 4, 6, 8, 0]
        checksum = sum([x * digits[i] for i, x in enumerate(coeff)]) % 11 % 10
        if checksum == digits[-1]:
            return True
        else:
            return False
    else:
        return False


def check_kpp(kpp):
    kpp = str(kpp)
    digits = [int(x) for x in kpp]
    if len(digits) == 9:
        return True
    else:
        return False
