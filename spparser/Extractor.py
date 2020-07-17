from .regex import RegexUtils
from .xpath import XpathUtils
from .css import CssUtils
from .utils.retObjects import retObjects
import re

class Extractor(object):
    @staticmethod
    def regex(pattern, string, flags=0, trim_mode=False, return_all=False, **kwargs):
        rutils = RegexUtils(pattern=pattern, string=string, flags=flags, trim_mode=trim_mode)
        if not return_all:
            res = rutils.extract_first()
        else:
            res = rutils.extract_all()
        return res

    @staticmethod
    def re_compile(pattern, flags=0):
        return re.compile(pattern, flags)

    @staticmethod
    def xpath(xpath_exp, text, return_all=False, **kwargs):
        xutils = XpathUtils(xpath_exp=xpath_exp, text=text)
        if not return_all:
            res = xutils.extract_first()
        else:
            res = xutils.extract_all()
        return res
    
    @staticmethod
    def css(css_exp, text, result_type="text", attr_name=None,remove_tags_exp=None, return_all=False,**kwargs):
        cssutils = CssUtils(css_exp=css_exp, text=text, result_type=result_type, attr_name=attr_name, remove_tags_exp=remove_tags_exp)
        if not return_all:
            res = cssutils.extract_first()
        else:
            res = cssutils.extract_all()
        return res

