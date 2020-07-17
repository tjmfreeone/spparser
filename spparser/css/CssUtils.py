from pyquery import PyQuery as pq
from ..utils import Exceptions

class CssUtils(object):
    def __init__(self, css_exp, text, result_type="text", attr_name=None, remove_tags_exp=None):
        self.css_exp = css_exp
        self.text = text
        self.result_type = result_type
        self.attr_name = attr_name
        self.remove_tags_exp = remove_tags_exp
        if result_type not in ["text", "html", "attr"]:
            raise Exceptions.ArgValueError('result_type must be "text", "html" or "attr"') 

    def extract_first(self):
        doc = pq(self.text)
        pos = doc(self.css_exp)
        if not pos:
            return None
        if self.result_type == "html":
            for i in pos.items():
                if self.remove_tags_exp:
                    i = i.remove(self.remove_tags_exp)
                return i.html()

        elif self.result_type == "text":
            for i in pos.items():
                if self.remove_tags_exp:
                    i = i.remove(self.remove_tags_exp)
                return i.html()

        elif self.result_type == "attr":
            res = pos.attr(self.attr_name)
            return res

    def extract_all(self):
        doc = pq(self.text)
        pos = doc.find(self.css_exp)
        res = []
        if self.result_type == "html":
            for i in pos.items():
                if self.remove_tags_exp:
                    i = i.remove(self.remove_tags_exp)
                res.append(i.html())
            return res

        elif self.result_type == "text":
            for i in pos.items():
                if self.remove_tags_exp:
                    i = i.remove(self.remove_tags_exp)
                res.append(i.text())
            return res

        elif self.result_type == "attr":
            for i in pos.items():
                res.append(i.attr(self.attr_name))
            return res

