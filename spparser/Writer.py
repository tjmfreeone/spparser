import csv
import json
from .utils import Exceptions


class Writer(object):
    @staticmethod
    def write_csv(data, file_path, mode="w", newline=None, each_line_type="dict", headers=None, encoding="utf-8"):
        if not isinstance(data, list):
            raise Exceptions.ArgValueError('data must be list type')
        if each_line_type not in ["list", "dict"]:
            raise  Exceptions.ArgValueError("each_line_type must be list or dict")
        
        with open(file=file_path, mode=mode, encoding=encoding, newline=newline) as f:
            if each_line_type == "list":
                csv_iter = csv.writer(f)
                if headers:
                    csv_iter.writerow(headers)
                for line in data:
                    csv_iter.writerow(line)
                    
            elif each_line_type == "dict":
                if not headers:
                    headers = list(data[0].keys())
                csv_iter = csv.DictWriter(f,fieldnames=headers)
                csv_iter.writeheader()
                for line in data:
                    csv_iter.writerow(line)

    @staticmethod
    def write_json(data, file_path, mode="w", compress=False, encoding='utf-8', indent=2, sort_keys=True, ensure_ascii=False):
        if not isinstance(data, dict) and not isinstance(data,list):
            raise Exceptions.ArgValueError('data must be dict type')
        with open(file_path, mode=mode, encoding=encoding) as f:
            if compress:
                json.dump(data, f)
            else:
                json.dump(data, f, indent=indent,sort_keys=sort_keys, ensure_ascii=ensure_ascii)

    @staticmethod
    def write_anyfile(data, file_path, mode="w", encoding="utf=-8"):
        if not isinstance(data, str):
            raise Exceptions.ArgValueError('data must be str type')
        with open(file_path, mode=mode, encoding=encoding) as f:
            f.write(data)
