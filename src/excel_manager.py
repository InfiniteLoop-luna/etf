"""Excel管理器 - 动态识别section并更新数据"""

import openpyxl
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional


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
        self.logger = logging.getLogger(__name__)
        self.wb = openpyxl.load_workbook(file_path)
        self.ws = self.wb.active
        self.sections = self._detect_sections()

    def _detect_sections(self) -> Dict[str, Section]:
        """动态扫描并识别所有section"""
        sections = {}
        current_section = None

        # 特殊处理第一个section（总市值）
        # 第一行是标题行，第二行是日期行，第三行开始是数据
        first_section_name = self.ws.cell(self.HEADER_ROW, self.DATA_START_COL).value
        if first_section_name and isinstance(first_section_name, str):
            current_section = Section(
                name=first_section_name,
                header_row=self.HEADER_ROW,
                data_start=self.HEADER_ROW + 2,  # 跳过标题行和日期行
                data_end=None
            )

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
        from datetime import datetime

        # 在DATE_ROW查找日期
        for col in range(self.DATA_START_COL, self.ws.max_column + 1):
            date_val = self.ws.cell(self.DATE_ROW, col).value

            # 处理datetime对象
            if isinstance(date_val, datetime):
                date_str = date_val.strftime('%Y-%m-%d')
                if date_str == target_date:
                    return col
            # 处理字符串
            elif isinstance(date_val, str):
                if date_val == target_date:
                    return col

        # 未找到，在最后添加新列
        new_col = self.ws.max_column + 1
        # 将日期作为datetime对象存储，保持与现有格式一致
        date_obj = datetime.strptime(target_date, '%Y-%m-%d')
        self.ws.cell(self.DATE_ROW, new_col, date_obj)
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

    def _find_etf_row(self, code: str, section: Section) -> Optional[int]:
        """在指定section中查找ETF行"""
        for row in range(section.data_start, section.data_end + 1):
            if self.ws.cell(row, self.CODE_COL).value == code:
                return row
        return None

    def save(self):
        """保存Excel文件"""
        self.wb.save(self.file_path)
        self.logger.info(f"Excel文件已保存: {self.file_path}")
