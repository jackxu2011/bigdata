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
a:{03.txt}
banana:{03.txt}
is:{02.txt, 01.txt, 03.txt}
it:{02.txt, 01.txt, 03.txt}
what:{01.txt, 02.txt}
```
运行 InvertedIndex02
```
a:{(03.txt,1)}
banana:{(03.txt,1)}
is:{(02.txt,1), (01.txt,2), (03.txt,1)}
it:{(02.txt,1), (01.txt,2), (03.txt,1)}
what:{(01.txt,1), (02.txt,1)}
```

## 作业二， modify from the repo
https://github.com/CoxAutomotiveDataSolutions/spark-distcp