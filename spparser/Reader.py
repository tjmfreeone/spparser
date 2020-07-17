import csv
import json
from .utils import Exceptions

class Reader(object):
    @staticmethod
    def read_csv(file_path, mode="r", newline=None, each_line_type="dict", start_line=1, max_read_lines=None, encoding="utf-8", **kwargs):
        if each_line_type not in ["list", "dict"]:
            raise  Exceptions.ArgValueError("each_line_type must be list or dict")
        res = []
        read_lines_count = 0
        with open(file=file_path, mode=mode, encoding=encoding, newline=newline) as f:
            if each_line_type == "list":
                csv_iter = csv.reader(f)
                for line in csv_iter:
                    if csv_iter.line_num < start_line:
                        continue
                    if not max_read_lines and max_read_lines != 0:
                        res.append(line)
                        continue
                    read_lines_count += 1
                    if read_lines_count <= max_read_lines:
                        res.append(line)
                    else:
                        break

            elif each_line_type == "dict":
                csv_iter = csv.DictReader(f)
                for line in csv_iter:
                    if csv_iter.line_num < start_line:
                        continue
                    if not max_read_lines and max_read_lines != 0:
                        res.append(dict(line))
                        continue
                    read_lines_count += 1
                    if read_lines_count <= max_read_lines:
                        res.append(dict(line))
                    else:
                        break
        return res

        
    @staticmethod
    def read_json(file_path, encoding="utf-8"):
        with open(file_path,encoding=encoding) as f:
            res = json.load(f)
        return res

    @staticmethod
    def read_anyfile(file_path, mode="r",newline=None, start_line=1, max_read_lines=None, line_by_line=False,encoding="utf-8", **kwargs):
        res = []
        read_lines_count = 0
        with open(file=file_path, mode=mode, encoding=encoding, newline=newline) as f:
            if not max_read_lines and max_read_lines != 0:
                if not line_by_line:
                    res = f.read()
                else:
                    for line in f.readlines():
                        res.append(line.strip("\n"))
                return res

            for i in range(1, max_read_lines+start_line+1):
                if i < start_line:
                    temp = f.readline()
                    if not temp:
                        break
                    continue

                if read_lines_count < max_read_lines:
                    read_lines_count += 1
                    temp = f.readline()
                    if not temp:
                        break
                    res.append(temp)
                else:
                    break
            if not line_by_line:
                res = "".join(res)
        return res
