import lxml
from lxml import etree
from lxml.etree import HTMLParser
from ..utils import Exceptions

class XpathUtils(object):
    def __init__(self, xpath_exp, text, parser_type="html"):
        self.xpath_exp = xpath_exp
        self.text = text
        self.parser_type = parser_type
        self.parser_type_map = {
                                 "html": HTMLParser(),
                                      }
        if self.parser_type not in self.parser_type_map.keys():
            raise Exceptions.XpathParserTypeError

    def extract_first(self):
        html = etree.HTML(self.text, self.parser_type_map[self.parser_type])
        res = html.xpath(self.xpath_exp)
        if not res:
            return None
        return res[0]

    def extract_all(self):
        html = etree.HTML(self.text, self.parser_type_map[self.parser_type])
        res = html.xpath(self.xpath_exp)
        if not res:
            return None
        return res

