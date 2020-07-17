from spparser import Reader, Writer, Extractor

def main():
    data = Reader.read_csv(file_path="./f.csv", each_line_type="dict", max_read_lines=10)
    '''
    example.csv:
    field1,field2
    1,2
    3,4
    5,6
    '''
    '''
    read_csv result: data = 
    '''
    alist = []
    data =[{'a': '122github', 'b': '2'}, {'a': '-8github999', 'b': '4'}]
    for item in data:
        res = Extractor.regex(r"[a-zA-Z]+", item["a"], flags=0, trim_mode=False, return_all=False)
        print(res)
        alist.append(res)
    Writer.write_json(alist,"result.json")

    res = Reader.read_anyfile("demo.html",start_line=1,max_read_lines=500,line_by_line=False)
    res = Extractor.xpath("//title/text()",res)
    print(res)

if __name__ == "__main__":
    main()
