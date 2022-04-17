## 作业一：
input files in resources input file
```
01.txt “it is what it is”
02.txt “what is it”
03.txt “it is a banana”
```
我们就能得到下面的反向文件索引：
运行： InvertedIndex
```
a:Set(03.txt)
banana:Set(03.txt)
is:Set(03.txt, 02.txt, 01.txt)
it:Set(03.txt, 02.txt, 01.txt)
what:Set(02.txt, 01.txt)
```
运行 InvertedIndex02
```
a:Set((03.txt,1))
banana:Set((03.txt,1))
is:Set((03.txt,1), (01.txt,2), (02.txt,1))
it:Set((03.txt,1), (01.txt,2), (02.txt,1))
what:Set((01.txt,1), (02.txt,1))
```

## 作业二， modify from the repo
https://github.com/CoxAutomotiveDataSolutions/spark-distcp