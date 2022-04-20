from .c_api import mkr_get_error_string


def check_error(error):
    if error == 0:
        return

    raise Exception("Error raised from api: " + mkr_get_error_string().decode("utf-8"))
