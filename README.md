# py-mongo-postgresql

### Overview

| 文件              | 功能         | 描述                                                         |
| :---------------- | :----------- | :----------------------------------------------------------- |
| config/config.ini | 配置         | Mongo、ES 配置项                                             |
| core/sync.py      | 核心同步模块 | 封装了mongo->pg 全量同步、增量同步等功能                              |
| core/process.py   | 业务逻辑模块 | 处理业务逻辑，供核心同步模块调用                             |
| utils/\*          | 工具类       | 放了一些轮子会用到的工具类，比如：时间处理，字符串格式化等等 |
| logs              | 日志         | 用来存放日志的                                               |
| requirements.txt  | 项目依赖包   | 熟悉python的朋友都懂~                                        |

### Run

```
cd py-mongo-postgresql/

python3 -m venv venv
source venv/bin/activate
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple --upgrade pip
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

python core/init.py
python core/sync.py
```

END!