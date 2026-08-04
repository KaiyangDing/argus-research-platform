"""M0.2 骨架冒烟：五层分层包 + mcp_servers 存在且可导入（施工手册 M0.2 测试清单）。"""


def test_packages_importable() -> None:
    import argus
    import argus.apps
    import argus.bus
    import argus.core
    import argus.engine
    import argus.roster
    import argus.tools
    import mcp_servers

    modules = [
        argus,
        argus.apps,
        argus.bus,
        argus.core,
        argus.engine,
        argus.roster,
        argus.tools,
        mcp_servers,
    ]
    assert all(m.__name__ for m in modules)
