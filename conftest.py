"""仓库根 conftest（测试基建，M1.5 起）：把项目根目录钉进 sys.path。

pytest 的 prepend 导入模式会把本文件所在目录插入 sys.path——tests/ 无 __init__.py
（M0 布局），tests.support 等跨目录导入按 PEP 420 命名空间包解析，前提是根目录
在 sys.path 上；本文件的存在保证这一点，别删。
"""
