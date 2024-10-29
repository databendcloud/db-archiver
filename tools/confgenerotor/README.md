# 配置文件生成器

这是一个用于生成数据同步配置文件的命令行工具。该工具基于现有的配置模板，根据输入参数更新特定字段并生成新的配置文件。

## 功能特性

- 基于模板生成配置文件
- 支持自定义数据库名和表名
- 支持按天、周、月设置时间范围
- 自动生成时间条件查询语句
- 保持模板中的其他配置不变

## 使用方法

### 命令行参数

```bash
./config-generator --sourceDb <数据库名> --sourceTable <表名>  --targetDbTable <目标数据库表名> --timeunit <时间单位> [-template <模板配置路径>]
```

# 示例

```bash
./config-generator --template config/conf.json --sourceDb db22 --sourceTable test22 --targetDbTable dd.tt --timeunit week
```