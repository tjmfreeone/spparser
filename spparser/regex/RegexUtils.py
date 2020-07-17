import re


class RegexUtils(object):
    def __init__(self, pattern, string, flags, trim_mode):
        self.pattern = pattern
        self.string = string
        self.flags = flags
        self.trim_mode = trim_mode

    def extract_first(self):
        it = re.finditer(self.pattern, self.string, self.flags)
        if not it:
            return None
        for i in it:
            if self.trim_mode:
                return i.groups()
            else:
                return i.group()

    def extract_all(self):
        if self.trim_mode:
            res = re.findall(self.pattern, self.string, self.flags)
            if not res:
                return None
            return res
        else:
            it = re.finditer(self.pattern, self.string, self.flags)
            if not it:
                return None
            res = []
            for i in it:
                res.append(i.group())
            return res

    #TODO Regular substitution
