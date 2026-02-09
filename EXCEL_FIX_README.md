# Excel文件修复说明

## 问题原因

Excel文件在数据更新后无法打开的原因是：
1. `openpyxl`库在保存文件时不会生成`calcChain.xml`文件
2. 尝试从旧文件中复制`calcChain.xml`会导致格式错误，因为旧的calcChain与新数据不匹配

## 解决方案

**最佳方案：不保留calcChain.xml，让Excel自动重新生成**

- Excel会在打开文件时自动重新生成`calcChain.xml`
- 这样可以确保calcChain与实际公式完全匹配
- 文件大小会稍小（约350KB），但功能完全正常

## 已修复的代码

修改了`src/excel_manager.py`中的`save()`方法：
- 移除了保留calcChain.xml的逻辑
- 设置了正确的Excel计算标志
- 添加了自动备份功能
- 使用临时文件确保保存安全

## 使用方法

直接运行数据更新程序即可：
```bash
python main.py
```

或指定日期：
```bash
python main.py --date 2026-02-09
```

## 文件说明

- `主要ETF基金份额变动情况.xlsx` - 主文件（会被更新）
- `主要ETF基金份额变动情况.org.xlsx` - 原始备份文件（不会被修改）
- `主要ETF基金份额变动情况.backup.YYYYMMDD_HHMMSS.xlsx` - 自动备份文件

## 注意事项

1. 第一次用Excel打开更新后的文件时，Excel会提示"正在计算"，这是正常的
2. Excel会自动重新生成calcChain.xml并保存
3. 之后再打开文件就会很快了

## 测试文件

如果当前文件仍然无法打开，可以尝试：
1. 使用`主要ETF基金份额变动情况_no_calcchain.xlsx`（已创建）
2. 或者从`主要ETF基金份额变动情况.org.xlsx`重新开始

## 技术细节

Excel文件（.xlsx）实际上是一个ZIP压缩包，包含多个XML文件：
- `xl/worksheets/sheet1.xml` - 工作表数据
- `xl/calcChain.xml` - 公式计算链（可选，Excel会自动生成）
- `xl/styles.xml` - 样式信息
- `[Content_Types].xml` - 内容类型定义
- 等等

`openpyxl`库可以正确处理除calcChain.xml外的所有文件。
