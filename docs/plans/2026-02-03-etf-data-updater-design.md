# ETF数据自动更新系统设计文档

**日期**: 2026-02-03
**版本**: 1.0
**状态**: 设计完成，待实现

## 1. 概述

### 1.1 目标

设计并实现一个Python脚本系统，用于自动获取ETF基金的市场数据并更新到Excel文件中。系统需要支持：

- 自动从多个数据源获取ETF总市值和单位市值数据
- 智能检测交易日，非交易日自动跳过
- 动态识别Excel文件中的多个数据section
- 支持新增ETF时自动扩展
- 多数据源容错机制
- 保留Excel中的计算公式

### 1.2 Excel文件结构

文件包含多个section，每个section由标题行和数据行组成：

**需要更新的数据Section（从数据源获取）：**
1. 总市值（亿元）
2. 基金单位市值

**计算型Section（包含公式，自动计算）：**
3. 基金份额
4. 基金份额变动
5. 基金申赎净额
6. 基金份额变动比例
7. 基金市值变动
8. 基金市值涨跌幅

**列结构：**
- 第0列：ETF代码（如 SH510300）
- 第1列：ETF名称（如 沪深300ETF）
- 第2列起：各交易日期的数据

## 2. 系统架构

### 2.1 整体架构

系统采用分层架构设计：

```
┌─────────────────────────────────────┐
│         Main Orchestrator           │
│   (协调整体流程，错误处理)            │
└─────────────────────────────────────┘
           │
           ├──────────────┬──────────────┬──────────────┐
           │              │              │              │
           ▼              ▼              ▼              ▼
    Trading Day      Data Source    Excel Manager   Logging
     Service          Manager                        System
```

### 2.2 核心组件

#### 2.2.1 Main Orchestrator（主协调器）
- 协调整体工作流程
- 处理命令行参数
- 统一错误处理和日志记录
- 生成执行报告

#### 2.2.2 Trading Day Service（交易日服务）
- 判断指定日期是否为交易日
- 支持多数据源查询（优先级fallback）
- 缓存交易日历数据

#### 2.2.3 Data Source Manager（数据源管理器）
- 管理多个数据源适配器
- 实现优先级fallback机制
- 统一数据接口

#### 2.2.4 Excel Manager（Excel管理器）
- 动态识别section结构
- 读取ETF列表
- 更新数据（保留公式）
- 支持新增ETF和日期列

### 2.3 工作流程

```
1. 确定目标日期（默认今天，或指定日期）
   ↓
2. 检查是否为交易日
   ├─ 是 → 继续
   └─ 否 → 记录日志，优雅退出
   ↓
3. 加载Excel文件，动态识别section
   ↓
4. 获取所有ETF代码列表
   ↓
5. 对每个ETF：
   ├─ 尝试数据源1获取数据
   ├─ 失败 → 尝试数据源2
   ├─ 失败 → 尝试数据源3
   └─ 成功 → 更新Excel
   ↓
6. 保存Excel文件（保留公式）
   ↓
7. 生成并打印执行报告
```

## 3. 数据源层设计

### 3.1 抽象接口

```python
from abc import ABC, abstractmethod

class ETFDataSource(ABC):
    """ETF数据源抽象基类"""

    @abstractmethod
    def get_etf_data(self, code: str, date: str) -> dict:
        """
        获取ETF数据

        Args:
            code: ETF代码（如 SH510300）
            date: 日期（格式：YYYY-MM-DD）

        Returns:
            {'market_value': float, 'unit_price': float}

        Raises:
            DataSourceError: 数据获取失败
        """
        pass

    @abstractmethod
    def is_trading_day(self, date: str) -> bool:
        """
        检查是否为交易日

        Args:
            date: 日期（格式：YYYY-MM-DD）

        Returns:
            bool: True表示是交易日
        """
        pass
```

### 3.2 具体实现（按优先级）

#### 3.2.1 AkShareSource（优先级1）
- **库**: `akshare`
- **API**: `ak.fund_etf_hist_sina()` 获取ETF历史数据
- **交易日**: `ak.tool_trade_date_hist_sina()` 获取交易日历
- **优点**: 免费，无需认证，维护活跃
- **缺点**: 可能有速率限制，依赖新浪后端

#### 3.2.2 TushareSource（优先级2）
- **库**: `tushare`
- **认证**: 需要API token（配置文件中设置）
- **API**: `pro.fund_nav()` 获取基金净值数据
- **优点**: 数据可靠，专业金融数据
- **缺点**: 需要注册和token

#### 3.2.3 EastmoneyScraperSource（优先级3）
- **方法**: 网页爬虫
- **目标**: eastmoney.com ETF页面
- **库**: `requests` + `BeautifulSoup`
- **优点**: 无需认证
- **缺点**: 易受HTML变化影响，速度较慢

#### 3.2.4 SinaScraperSource（优先级4）
- **方法**: 网页爬虫
- **目标**: sina.com.cn 财经页面
- **用途**: 最后备用方案

### 3.3 数据源管理器

```python
class DataSourceManager:
    """数据源管理器，实现优先级fallback"""

    def __init__(self, sources: list[ETFDataSource]):
        self.sources = sources  # 按优先级排序
        self.logger = logging.getLogger(__name__)

    def fetch_data(self, code: str, date: str) -> dict:
        """
        按优先级尝试获取数据，自动fallback

        Returns:
            {'market_value': float, 'unit_price': float}

        Raises:
            DataFetchError: 所有数据源都失败
        """
        for source in self.sources:
            try:
                self.logger.info(f"尝试使用 {source.__class__.__name__}")
                data = source.get_etf_data(code, date)
                self.logger.info(f"✓ {source.__class__.__name__} 成功")
                return data
            except Exception as e:
                self.logger.warning(
                    f"✗ {source.__class__.__name__} 失败: {e}"
                )
                continue

        raise DataFetchError(f"所有数据源获取 {code} 数据失败")

    def is_trading_day(self, date: str) -> bool:
        """检查是否为交易日（使用第一个可用的数据源）"""
        for source in self.sources:
            try:
                return source.is_trading_day(date)
            except Exception:
                continue
        # 如果所有数据源都失败，假设是交易日（保守策略）
        self.logger.warning("无法确定交易日，假设为交易日")
        return True
```

## 4. Excel管理器设计

### 4.1 Section数据结构

```python
from dataclasses import dataclass

@dataclass
class Section:
    """Excel中的一个section"""
    name: str           # Section名称（如"总市值：亿元"）
    header_row: int     # 标题行号
    data_start: int     # 数据起始行号
    data_end: int       # 数据结束行号

    @property
    def is_calculated(self) -> bool:
        """判断是否为计算型section"""
        calculated_keywords = ['份额', '变动', '申赎', '比例', '涨跌幅']
        return any(kw in self.name for kw in calculated_keywords)

    @property
    def is_data_section(self) -> bool:
        """判断是否为需要更新的数据section"""
        return '总市值' in self.name or '基金单位市值' in self.name
```

### 4.2 动态Excel管理器

```python
import openpyxl
from typing import Dict, List

class DynamicExcelManager:
    """动态Excel管理器，自动识别section结构"""

    # 常量定义
    HEADER_ROW = 1
    DATE_ROW = 2
    CODE_COL = 1
    NAME_COL = 2
    DATA_START_COL = 3

    def __init__(self, file_path: str):
        self.file_path = file_path
        self.wb = openpyxl.load_workbook(file_path)
        self.ws = self.wb.active
        self.sections = self._detect_sections()
        self.logger = logging.getLogger(__name__)

    def _detect_sections(self) -> Dict[str, Section]:
        """动态扫描并识别所有section"""
        sections = {}
        current_section = None

        for row_idx in range(1, self.ws.max_row + 1):
            if self._is_section_header(row_idx):
                # 结束上一个section
                if current_section:
                    current_section.data_end = row_idx - 1
                    sections[current_section.name] = current_section

                # 开始新section
                section_name = self.ws.cell(row_idx, self.NAME_COL).value
                current_section = Section(
                    name=section_name,
                    header_row=row_idx,
                    data_start=row_idx + 1,
                    data_end=None
                )

        # 处理最后一个section
        if current_section:
            current_section.data_end = self.ws.max_row
            sections[current_section.name] = current_section

        self.logger.info(f"检测到 {len(sections)} 个section")
        return sections

    def _is_section_header(self, row: int) -> bool:
        """判断是否为section标题行"""
        code_cell = self.ws.cell(row, self.CODE_COL).value
        name_cell = self.ws.cell(row, self.NAME_COL).value

        # Section header特征：第0列为空，第1列包含关键词
        keywords = ['市值', '份额', '变动', '申赎', '比例', '涨跌幅']
        return (code_cell is None and
                name_cell and
                isinstance(name_cell, str) and
                any(kw in name_cell for kw in keywords))

    def get_etf_codes(self) -> List[str]:
        """从第一个数据section获取所有ETF代码"""
        # 找到第一个数据section
        data_section = next(
            (s for s in self.sections.values() if s.is_data_section),
            None
        )

        if not data_section:
            raise ValueError("未找到数据section")

        codes = []
        for row in range(data_section.data_start, data_section.data_end + 1):
            code = self.ws.cell(row, self.CODE_COL).value
            if code and isinstance(code, str):
                codes.append(code)

        return codes

    def find_or_create_date_column(self, target_date: str) -> int:
        """查找或创建日期列"""
        # 在DATE_ROW查找日期
        for col in range(self.DATA_START_COL, self.ws.max_column + 1):
            date_val = self.ws.cell(self.DATE_ROW, col).value
            if str(date_val) == target_date:
                return col

        # 未找到，在最后添加新列
        new_col = self.ws.max_column + 1
        self.ws.cell(self.DATE_ROW, new_col, target_date)
        self.logger.info(f"创建新日期列: {target_date} (列{new_col})")
        return new_col

    def update_data(self, code: str, date: str,
                   market_value: float, unit_price: float):
        """更新指定ETF的数据"""
        col_idx = self.find_or_create_date_column(date)

        # 只更新数据section
        for section in self.sections.values():
            if not section.is_data_section:
                continue

            row_idx = self._find_etf_row(code, section)
            if row_idx is None:
                self.logger.warning(
                    f"在section '{section.name}' 中未找到 {code}"
                )
                continue

            # 根据section类型更新对应的值
            value = market_value if '总市值' in section.name else unit_price
            self.ws.cell(row_idx, col_idx, value)
            self.logger.debug(
                f"更新 {code} {section.name}: {value}"
            )

    def _find_etf_row(self, code: str, section: Section) -> int:
        """在指定section中查找ETF行"""
        for row in range(section.data_start, section.data_end + 1):
            if self.ws.cell(row, self.CODE_COL).value == code:
                return row
        return None

    def save(self):
        """保存Excel文件"""
        self.wb.save(self.file_path)
        self.logger.info(f"Excel文件已保存: {self.file_path}")
```

## 5. 错误处理与日志

### 5.1 异常层次结构

```python
class ETFDataError(Exception):
    """ETF数据相关的基础异常"""
    pass

class DataSourceError(ETFDataError):
    """数据源获取失败"""
    pass

class DataFetchError(DataSourceError):
    """所有数据源都失败"""
    pass

class TradingDayError(ETFDataError):
    """非交易日错误"""
    pass

class ExcelUpdateError(ETFDataError):
    """Excel更新失败"""
    pass
```

### 5.2 日志配置

```python
import logging
from logging.handlers import RotatingFileHandler
import os

def setup_logging(log_level: str = 'INFO') -> logging.Logger:
    """配置日志系统"""
    logger = logging.getLogger('etf_updater')
    logger.setLevel(getattr(logging, log_level.upper()))

    # 创建logs目录
    os.makedirs('logs', exist_ok=True)

    # 文件日志（轮转，保留10个文件，每个10MB）
    file_handler = RotatingFileHandler(
        'logs/etf_updater.log',
        maxBytes=10*1024*1024,
        backupCount=10,
        encoding='utf-8'
    )
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    ))

    # 控制台日志
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(
        '%(levelname)s: %(message)s'
    ))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
```

### 5.3 执行报告

```python
from dataclasses import dataclass, field
from typing import List, Dict

@dataclass
class ExecutionReport:
    """执行结果报告"""
    success_count: int = 0
    failed_etfs: List[Dict[str, str]] = field(default_factory=list)
    skipped_reason: str = None

    def add_success(self, code: str):
        """记录成功"""
        self.success_count += 1

    def add_failure(self, code: str, error: str):
        """记录失败"""
        self.failed_etfs.append({'code': code, 'error': error})

    def print_summary(self):
        """打印执行摘要"""
        if self.skipped_reason:
            print(f"\n✓ 跳过执行: {self.skipped_reason}\n")
            return

        print(f"\n{'='*60}")
        print(f"执行完成")
        print(f"{'='*60}")
        print(f"成功更新: {self.success_count} 个ETF")

        if self.failed_etfs:
            print(f"失败: {len(self.failed_etfs)} 个ETF")
            for item in self.failed_etfs:
                print(f"  - {item['code']}: {item['error']}")
        else:
            print("✓ 所有ETF更新成功！")

        print(f"{'='*60}\n")
```

## 6. 主程序流程

```python
from datetime import datetime
import sys

def main(target_date: str = None) -> int:
    """
    主程序入口

    Args:
        target_date: 目标日期（YYYY-MM-DD），None表示今天

    Returns:
        int: 退出码（0=成功，1=部分失败，2=Excel错误，3=未知错误）
    """
    logger = setup_logging()
    report = ExecutionReport()

    try:
        # 1. 确定目标日期
        date = target_date or datetime.now().strftime('%Y-%m-%d')
        logger.info(f"{'='*60}")
        logger.info(f"ETF数据更新程序启动")
        logger.info(f"目标日期: {date}")
        logger.info(f"{'='*60}")

        # 2. 检查是否为交易日
        source_manager = DataSourceManager(load_data_sources())
        if not source_manager.is_trading_day(date):
            report.skipped_reason = f"{date} 不是交易日"
            logger.info(report.skipped_reason)
            report.print_summary()
            return 0

        logger.info(f"{date} 是交易日，开始更新数据")

        # 3. 初始化Excel管理器
        excel_manager = DynamicExcelManager('主要ETF基金份额变动情况.xlsx')

        # 4. 获取ETF列表
        etf_codes = excel_manager.get_etf_codes()
        logger.info(f"发现 {len(etf_codes)} 个ETF需要更新")

        # 5. 逐个更新ETF
        for idx, code in enumerate(etf_codes, 1):
            try:
                logger.info(f"[{idx}/{len(etf_codes)}] 正在获取 {code} 的数据...")

                # 获取数据
                data = source_manager.fetch_data(code, date)

                # 更新Excel
                excel_manager.update_data(
                    code=code,
                    date=date,
                    market_value=data['market_value'],
                    unit_price=data['unit_price']
                )

                report.add_success(code)
                logger.info(f"✓ {code} 更新成功")

            except DataFetchError as e:
                report.add_failure(code, str(e))
                logger.error(f"✗ {code} 数据获取失败: {e}")
                continue
            except Exception as e:
                report.add_failure(code, f"未知错误: {e}")
                logger.exception(f"✗ {code} 更新时发生异常")
                continue

        # 6. 保存Excel
        logger.info("正在保存Excel文件...")
        excel_manager.save()
        logger.info("✓ Excel文件保存成功")

        # 7. 打印报告
        report.print_summary()

        return 0 if not report.failed_etfs else 1

    except ExcelUpdateError as e:
        logger.error(f"Excel操作失败: {e}")
        return 2
    except Exception as e:
        logger.exception(f"未预期的错误: {e}")
        return 3

def load_data_sources() -> List[ETFDataSource]:
    """加载并初始化数据源"""
    # 从配置文件读取配置
    # 按优先级创建数据源实例
    # 返回数据源列表
    pass

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='ETF数据自动更新程序')
    parser.add_argument(
        '--date',
        type=str,
        help='目标日期（YYYY-MM-DD），默认为今天'
    )

    args = parser.parse_args()
    sys.exit(main(args.date))
```

## 7. 配置文件

### 7.1 config.yaml

```yaml
# ETF数据更新程序配置文件

# Excel文件路径
excel_file: "主要ETF基金份额变动情况.xlsx"

# 数据源配置（按优先级排序）
data_sources:
  akshare:
    enabled: true
    priority: 1
    timeout: 10  # 秒

  tushare:
    enabled: true
    priority: 2
    token: ""  # 需要填写Tushare API token
    timeout: 10

  eastmoney_scraper:
    enabled: true
    priority: 3
    timeout: 15
    user_agent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

  sina_scraper:
    enabled: false  # 默认禁用，作为最后备用
    priority: 4
    timeout: 15

# 日志配置
logging:
  level: INFO  # DEBUG, INFO, WARNING, ERROR
  file: "logs/etf_updater.log"
  max_bytes: 10485760  # 10MB
  backup_count: 10

# 重试配置
retry:
  max_attempts: 3
  delay_seconds: 1
```

## 8. 项目结构

```
etf/
├── main.py                          # 主程序入口
├── config.yaml                      # 配置文件
├── requirements.txt                 # Python依赖
├── README.md                        # 项目说明
├── 主要ETF基金份额变动情况.xlsx      # Excel数据文件
├── logs/                            # 日志目录
│   └── etf_updater.log
├── src/
│   ├── __init__.py
│   ├── data_sources/                # 数据源模块
│   │   ├── __init__.py
│   │   ├── base.py                  # 抽象基类
│   │   ├── akshare_source.py        # AkShare实现
│   │   ├── tushare_source.py        # Tushare实现
│   │   ├── eastmoney_scraper.py     # 东方财富爬虫
│   │   └── sina_scraper.py          # 新浪财经爬虫
│   ├── excel_manager.py             # Excel管理器
│   ├── data_source_manager.py       # 数据源管理器
│   ├── trading_day_service.py       # 交易日服务
│   ├── exceptions.py                # 异常定义
│   └── utils.py                     # 工具函数
└── docs/
    └── plans/
        └── 2026-02-03-etf-data-updater-design.md  # 本设计文档
```

## 9. 依赖包

```txt
# requirements.txt
openpyxl>=3.1.0
akshare>=1.12.0
tushare>=1.3.0
requests>=2.31.0
beautifulsoup4>=4.12.0
pyyaml>=6.0
```

## 10. 使用示例

### 10.1 日常使用（自动检测今天）

```bash
python main.py
```

输出示例：
```
INFO: 目标日期: 2026-02-03
INFO: 2026-02-03 是交易日，开始更新数据
INFO: 发现 15 个ETF需要更新
INFO: [1/15] 正在获取 SH510300 的数据...
INFO: ✓ SH510300 更新成功
...
INFO: ✓ Excel文件保存成功

============================================================
执行完成
============================================================
成功更新: 15 个ETF
✓ 所有ETF更新成功！
============================================================
```

### 10.2 指定日期更新

```bash
python main.py --date 2026-01-15
```

### 10.3 非交易日

```bash
python main.py --date 2026-02-01  # 假设是周六
```

输出：
```
INFO: 目标日期: 2026-02-01
INFO: 2026-02-01 不是交易日

✓ 跳过执行: 2026-02-01 不是交易日
```

### 10.4 作为模块使用

```python
from src.excel_manager import DynamicExcelManager
from src.data_source_manager import DataSourceManager

# 初始化
excel_mgr = DynamicExcelManager('主要ETF基金份额变动情况.xlsx')
source_mgr = DataSourceManager(load_data_sources())

# 获取数据并更新
data = source_mgr.fetch_data('SH510300', '2026-02-03')
excel_mgr.update_data('SH510300', '2026-02-03',
                     data['market_value'], data['unit_price'])
excel_mgr.save()
```

## 11. 扩展性设计

### 11.1 添加新ETF

只需在Excel文件的数据section中添加新行：
- 在"总市值"section添加一行（代码 + 名称）
- 在"基金单位市值"section添加对应行
- 程序会自动识别并更新

### 11.2 添加新数据源

1. 创建新的数据源类，继承`ETFDataSource`
2. 实现`get_etf_data()`和`is_trading_day()`方法
3. 在配置文件中添加配置
4. 在`load_data_sources()`中注册

### 11.3 添加新Section

Excel中添加新的计算section时：
- 如果包含公式，程序会自动识别为计算section，不会更新
- 如果需要程序更新，修改`Section.is_data_section`属性判断逻辑

## 12. 注意事项

1. **公式保护**: 使用`openpyxl`时不要使用`data_only=True`，以保留公式
2. **备份**: 建议定期备份Excel文件
3. **API限制**: 注意各数据源的API调用频率限制
4. **时区**: 所有日期使用本地时区
5. **编码**: 所有文件使用UTF-8编码
6. **权限**: 确保程序对Excel文件有读写权限

## 13. 后续优化方向

1. **并发获取**: 使用多线程/异步IO并发获取多个ETF数据
2. **增量更新**: 只更新缺失的日期，而不是每次都更新
3. **数据验证**: 添加数据合理性检查（如市值不能为负）
4. **通知机制**: 更新完成后发送邮件/微信通知
5. **Web界面**: 提供Web UI进行配置和监控
6. **Docker化**: 打包为Docker镜像，便于部署

---

**文档结束**