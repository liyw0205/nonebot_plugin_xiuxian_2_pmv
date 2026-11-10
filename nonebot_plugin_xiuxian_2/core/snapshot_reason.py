"""Select the first failed, already normalized business comparison."""


def first_difference(actual, expected, reasons):
    return next(reason for left, right, reason in zip(actual, expected, reasons) if left != right)
