
class FieldNameTypeError(Exception):
    def __str__(self):
        return repr("field_name must be str or list")
    
class XpathParserTypeError(Exception):
    def __str__(self):
        return repr("incorrect parser type")

class ArgValueError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

class ParamsError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)
