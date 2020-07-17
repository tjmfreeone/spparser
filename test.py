from spparser import Extractor
from lxml import etree
from pyquery import PyQuery as pq


if __name__ == "__main__":
    
    res = Extractor.regex(r"((炫诗.*?菁华油))((?!睡眠).)","炫诗菁华油sasasa炫诗菁华油s", return_all=True, trim_mode=False)
    print(res)
    
    text='''
    <div>
    <ul>
         <li class="item-0"><a href="link1.html">第一个</a></li>
         <li class="item-1"><a href="link2.html">second item</a></li>
         <li class="item-1"><a href="link2.html">third item</a></li>
         </ul>
     </div>
    '''

    res = Extractor.xpath('//li[@class="item-1"]/a/text()',text, return_all=True)
    print(res)

    text = '''
    <html lang="en">
    <head>
        <title>PyQuery学习</title>
    </head>
    <body>
        <ul id="container">
            <li class="object-1" tag="1"/>
            <li class="object-2"/>
            <li class="object-3"/>
        </ul>
    </body>
</html><html lang="en">
    <head>
        <title>PyQuery学习</title>
    </head>
    <body>
        <ul id="container">
            <li class="object-1" tag="2"/>
            <li class="object-2"/>
            <li class="object-3"/>
        </ul>
    </body>
    </html>
    '''

    #doc = pq(text)
    #print(type(doc('#container')))
    #print(not doc.find('#consta'))
    #for i in doc.find('#container').items():
    #    print("---")
    #    print(i.html())
    res = Extractor.css("li", text, return_all=False, result_type="attr",remove_tags_exp=None,attr_name="tag")
    print(res)
