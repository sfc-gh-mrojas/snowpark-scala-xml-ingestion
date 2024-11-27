create stage xml_files;

put file:///Users/mrojas/snowpark-scala-xml-ingestion/books.xml @xml_files auto_compress=False overwrite=true;

CREATE OR REPLACE FUNCTION EXTRACT_XML(FILE_PATH STRING, QUERY STRING, ITER BOOLEAN DEFAULT FALSE, OPTIONS OBJECT DEFAULT {})
RETURNS TABLE(DATA VARIANT)
LANGUAGE PYTHON
RUNTIME_VERSION='3.9'
PACKAGES = ('snowflake-snowpark-python','lxml')
HANDLER = 'XmlProcess'
AS
$$
import gzip
import tarfile
import zipfile
import re
from snowflake.snowpark.files import SnowflakeFile
from lxml import etree
class XmlProcess:
    def __init__(self):
        pass
    def opts(self,options):
        self.redacted        = options.get("redacted",[])
        self.array_elements  = options.get("array_elements",[])
        self.namespaces      = options.get("namespaces")
        self.omit_namespaces = options.get("omit_namespaces",[])
        self.text_attr       = options.get("text") or "#text"
        self.attr_prefix     = options.get("attr_prefix","@")
        self.get_attr        = lambda attr_name : self.attr_prefix + attr_name
        self.get_tag         = lambda tag_name : tag_name
        if options.get("tag","").lower()        == "with_namespace":
            nm_map = self.namespaces
            nm_separator = options.get("namespace_sep","")
            if nm_map:
                inv_nm_map = {v: k for k, v in nm_map.items()}
                def map_tag(tag_name):
                    qn=etree.QName(tag_name)
                    if qn.namespace in self.omit_namespaces:
                        return qn.localname
                    else:
                        return inv_nm_map.get(qn.namespace,qn.namespace) + nm_separator + qn.localname
                self.get_tag = map_tag
        if options.get("tag","").lower()        == "remove_namespace":
                def remove_ns(tag_name):
                    qn=etree.QName(tag_name)
                    return qn.localname
                self.get_tag = remove_ns
        if options.get("tag_case","").lower() == "upper":
            old = self.get_tag
            self.get_tag = lambda x : old(x).upper()
        if options.get("attribute_case","").lower() == "upper":
            self.get_attr = lambda attr_name : attr_name.upper()
    def element_to_dict(self, element):
        element_tag = self.get_tag(element.tag)
        if element_tag in self.array_elements:
            return [self.element_to_dict(child) for child in element]
        result = {self.get_attr(k): v for k, v in element.attrib.items()}
        for child in element:
            child_new_tag = self.get_tag(child.tag)
            result[child_new_tag] = self.element_to_dict(child)
        inner_text = (element.text or "").strip()
        element.clear()
        if len(result)==0: # Nothing added so far, well then everything we have is the inner_text
            result = inner_text
        elif inner_text:
            result[self.text_attr] = inner_text
        if isinstance(result,dict):
            for entry in self.redacted:
                result.pop(entry,None)
        return result 
    def extract_from_xml_xpath(self,xml_data, query):
        xml_text = str(xml_data,'utf-8')
        xml_data = None # Free memory
        RE_XML_ENCODING = re.compile(r'^(<\?xml[^>]+)\s+encoding\s*=\s*["\'][^"\']*["\'](\s*\?>|)', re.U)
        xml_text=RE_XML_ENCODING.sub("", xml_text, count=1)
        # Parse XML
        root = etree.fromstring(xml_text)
        # Apply Query expression
        results = root.xpath(query,namespaces=self.namespaces)
        for node in results:
            yield (self.element_to_dict(node),)
    def extract_from_xml_iter(self, xml_data, element_name):
        from io import BytesIO
        context = etree.iterparse(BytesIO(xml_data), tag=element_name)
        for event, element in context:
            yield (self.element_to_dict(element), )
    def extract_from_xml(self,xml_data, query, iter):
        if iter:
            yield from self.extract_from_xml_iter(xml_data,query)
        else:
            yield from self.extract_from_xml_xpath(xml_data,query)
    def process(self,file_path, query, iter, options):
        self.opts(options)
        with SnowflakeFile.open(file_path,'rb',require_scoped_url=False) as fhandle:
            if file_path.endswith(".zip"):
                with zipfile.ZipFile(fhandle,'r') as zip_file:
                    for file_info in zip_file.infolist():
                        if file_info.filename.endswith(".xml"):
                            with zip_file.open(file_info) as open_zip:
                                yield from self.extract_from_xml(xml_data,query,iter)
            elif file_path.endswith(".tar.gz") or file_path.endswith(".tgz"):
                with gzip.open(fhandle, 'rb') as gz_file:
                        with tarfile.open(fileobj=gz_file, mode='r:*') as tar:
                            for member in tar.getmembers():
                                if member.isfile() and member.name.endswith(".xml"):
                                    xml_data = tar.extractfile(member).read()
                                    # is it binary
                                    if xml_data[0] != 0:
                                        yield from self.extract_from_xml(xml_data, query,iter)
            elif file_path.endswith(".gz"):
                with gzip.open(fhandle, 'rb') as gz_file:
                    xml_data = gz_file.read()
                    yield from self.extract_from_xml_iter(xml_data, query,iter)
            else:
                xml_data = fhandle.read()
                yield from self.extract_from_xml(xml_data, query,iter)
$$;

put file:///Users/mrojas/snowpark-scala-xml-ingestion/books.xml @mystage auto_compress=False overwrite=True;


create or replace temp table results as 
select * from 
table(EXTRACT_XML('@xml_files/books.xml', 'book', true,{'attr_prefix':'_'}));

select * from results;

select ARRAY_UNIQUE_AGG(VALUE) KEYS from 
(select DISTINCT OBJECT_KEYS(DATA) KEYS from results) EXTRACTED_KEYS_TABLE,
table(flatten(input => EXTRACTED_KEYS_TABLE.KEYS));

select value from
  (select ARRAY_AGG(DISTINCT OBJECT_KEYS(DATA)) ALL_KEYS from results) all_keys_table,
  table(flatten(input => all_keys_table.ALL_KEYS));
