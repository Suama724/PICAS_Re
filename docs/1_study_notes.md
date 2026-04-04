# .XML 文件

其应该具有这种形式:
```xml
<root>
<child1>str content</child1>
<>
</root>
```
```xml
<article mdate="2002-01-03" key="persons/Codd71a">
<author>E. F. Codd</author>
<title>Further Normalization of the Data Base Relational Model.</title>
<journal>IBM Research Report, San Jose, California</journal>
<volume>RJ909</volume>
<month>August</month>
<year>1971</year>
<cdrom>ibmTR/rj909.pdf</cdrom>
<ee>db/labs/ibm/RJ909.html</ee>
</article>
```
其中标签中可以写attr, 它们相当于作为元元tag, 其信息与论文本身(基本)不沾, 只是方便管理检索
处理xml: xml.etree.ElementTree 库 (貌似不太好, 他会作为一棵树整体加载到内存里)
用sax?

python有两种方法: dom(Document object model) sax (simple api for xml)
dom 需要将整个文件放进内存, 似乎没法处理持续增长的文件?
sax可以读一删一(不太好写?)
然后混合有Iterparse, 分块处理等

信源: 定义 https://zh.wikipedia.org/wiki/XML
      库 https://blog.csdn.net/qq233325332/article/details/130799948

# 包内相互import 

如test内部的init写法，统一从`from T0x...`改成`from .T0x...`

# lxml
它因为是底层c实现的，加一个types-lxml才能正常自动补全

