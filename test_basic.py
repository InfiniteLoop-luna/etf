# -*- coding: utf-8 -*-
"""简单测试脚本 - 验证系统组件"""

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.path.insert(0, '.')

from src.excel_manager import DynamicExcelManager, Section
from src.data_sources.base import ETFDataSource
from src.exceptions import ETFDataError, DataSourceError

print("Test 1: Import modules...")
print("[OK] All modules imported successfully")

print("\nTest 2: Section dataclass...")
section = Section(name="Total Market Value", header_row=1, data_start=2, data_end=15)
print(f"  Section name: {section.name}")
print(f"  Is data section: {section.is_data_section}")
print(f"  Is calculated section: {section.is_calculated}")
print("[OK] Section dataclass works")

print("\nTest 3: Exception classes...")
try:
    raise DataSourceError("Test exception")
except ETFDataError as e:
    print(f"  Caught exception: {e}")
print("[OK] Exception classes work")

print("\nTest 4: Excel Manager...")
try:
    excel_mgr = DynamicExcelManager('主要ETF基金份额变动情况.xlsx')
    print(f"  Detected {len(excel_mgr.sections)} sections")
    etf_codes = excel_mgr.get_etf_codes()
    print(f"  Found {len(etf_codes)} ETFs")
    print(f"  ETF codes: {etf_codes[:3]}...")
    print("[OK] Excel Manager works")
except Exception as e:
    print(f"  [FAIL] Excel Manager test failed: {e}")

print("\nAll basic tests completed!")
