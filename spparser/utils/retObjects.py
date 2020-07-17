from .Exceptions import FieldNameTypeError

class retObjects(object):
    def retDict(field_names, field_values):
        adict = dict()
        if not field_names:
            adict["result"] = field_values
        elif isinstance(field_names, str):
            adict[field_names] = field_values
        elif isinstance(field_names, list):
            for field in field_names:
                adict[field] = field_values
        else:
            raise FieldNameTypeError
        return adict

        
