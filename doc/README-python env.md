### miniconda installation
download: https://mirrors.tuna.tsinghua.edu.cn/anaconda/miniconda/

---
### Q&A

#### CondaHTTPError
Run command: conda update conda
Prompt error: CondaHTTPError: HTTP 000 CONNECTION FAILED for url ...
##### Solution 1
copied the following files
libcrypto-1_1-x64.*
libssl-1_1-x64.*
from <Anaconda3 path>\Library\bin to <Anaconda3 path>\DLLs
##### Solution 2 (better)
Add <Anaconda3 path>\Library\bin into environment path
##### 原文参考
https://github.com/conda/conda/issues/9746#:~:text=CondaHTTPError%3A%20HTTP%20000%20CONNECTION%20FAILED%20for%20url%20https%3A%2F%2Frepo.anaconda.com%2Fpkgs%2Fmain%2Fwin-64%2Fcurrent_repodata.json,simple%20retry%20will%20get%20you%20on%20your%20way.

#### No module named 'pysqlite2'
##### Solution
1. put <Anaconda3 path>\Scripts into environment path
2. visit https://www.sqlite.org/download.html, download sqlite-dll-win64-x64-3380500.zip (for windows 10), unzip to sqlite3.*, put into <Anaconda3 path>\DLLs

#### numpy: "DLL load failed while importing _multiarray_umath" or "CondaHTTPError: HTTP 000 CONNECTION FAILED for url <https://repo.anaconda.com/pkgs/main/win-32/mkl-2021.4.0-h9f7ea03_640.conda>"
##### Solution 1
copy all Intel mkl*.dll libraries from <Anaconda3 path>\Library\bin into \Lib\site-packages\numpy\core folder
##### Solution 2 (better)
Add <Anaconda3 path>\Library\bin into environment path, restart cmd
##### 原文参考
https://github.com/ContinuumIO/anaconda-issues/issues/1508

---
### conda config

##### conda channel config
conda config --show-sources  ## C:\Users\Administrator\.condarc
conda config --set show_channel_urls yes
conda config --set ssl_verify no
conda config --add channels conda-forge
conda config --add channels https://mirrors.tuna.tsinghua.edu.cn/anaconda/pkgs/main/win-32/
conda config --remove-key channels
conda clean -i  ## 清除索引缓存

##### 清华Anaconda 镜像使用帮助
https://mirror.tuna.tsinghua.edu.cn/help/anaconda/

---
### new env

##### python env sample


---
### Jupyter
Jupyter lab: http://localhost:8888/lab
Jupyter notebook: http://localhost:8888/tree

---
### library
conda update conda
conda install numpy
conda install pandas
conda install jupyterlab

---

