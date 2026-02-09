"""使用xlwings的Excel管理器 - 完美保留所有Excel数据"""

import xlwings as xw
import logging
from datetime import datetime
from typing import List, Optional


class XlwingsExcelManager:
    """使用xlwings的Excel管理器，完美保留Excel格式和数据"""

    # 常量定义
    HEADER_ROW = 1
    DATE_ROW = 2
    CODE_COL = 1
    NAME_COL = 2
    DATA_START_COL = 3

    def __init__(self, file_path: str):
        self.file_path = file_path
        self.logger = logging.getLogger(__name__)

        # 启动Excel（不可见）
        self.app = xw.App(visible=False)
        self.app.display_alerts = False
        self.app.screen_updating = False

        # 打开工作簿
        self.wb = self.app.books.open(file_path)
        self.ws = self.wb.sheets[0]

        self.logger.info(f"已使用xlwings打开文件: {file_path}")

    def __del__(self):
        """清理资源"""
        try:
            if hasattr(self, 'wb'):
                self.wb.close()
            if hasattr(self, 'app'):
                self.app.quit()
        except:
            pass

    def get_etf_codes(self) -> List[str]:
        """从第一个数据section获取所有ETF代码"""
        codes = []

        # 从第3行开始读取代码（跳过标题行和日期行）
        row = 3
        while True:
            code = self.ws.range(f'A{row}').value
            if code is None:
                break
            if isinstance(code, str) and code.strip():
                codes.append(code.strip())
            row += 1

            # 检查是否到达section分隔符
            name_cell = self.ws.range(f'B{row}').value
            if name_cell and '市值' in str(name_cell):
                break

        self.logger.info(f"找到 {len(codes)} 个ETF代码")
        return codes

    def find_or_create_date_column(self, target_date: str) -> int:
        """查找或创建日期列"""
        # 在DATE_ROW查找日期
        col = self.DATA_START_COL
        max_col = self.ws.used_range.last_cell.column

        while col <= max_col + 1:
            date_val = self.ws.range((self.DATE_ROW, col)).value

            # 处理datetime对象
            if isinstance(date_val, datetime):
                date_str = date_val.strftime('%Y-%m-%d')
                if date_str == target_date:
                    self.logger.info(f"找到日期列: {target_date} (列{col})")
                    return col
            # 处理字符串
            elif isinstance(date_val, str):
                if date_val == target_date:
                    self.logger.info(f"找到日期列: {target_date} (列{col})")
                    return col
            # 如果是空列，说明到达末尾
            elif date_val is None and col > self.DATA_START_COL:
                break

            col += 1

        # 未找到，在最后添加新列
        new_col = col
        date_obj = datetime.strptime(target_date, '%Y-%m-%d')
        self.ws.range((self.DATE_ROW, new_col)).value = date_obj
        self.logger.info(f"创建新日期列: {target_date} (列{new_col})")
        return new_col

    def update_data(self, code: str, date: str, market_value: float, unit_price: float):
        """更新指定ETF的数据"""
        col_idx = self.find_or_create_date_column(date)

        # 找到ETF所在的行
        row = 3
        found = False

        while True:
            cell_code = self.ws.range(f'A{row}').value
            if cell_code is None:
                break

            if str(cell_code).strip() == code:
                # 找到了，更新总市值
                self.ws.range((row, col_idx)).value = market_value
                self.logger.debug(f"更新 {code} 总市值: {market_value}")
                found = True
                break

            row += 1

            # 检查是否到达section分隔符
            if row > 1000:  # 安全限制
                break

        if not found:
            self.logger.warning(f"未找到ETF代码: {code}")
            return

        # 更新基金单位市值（在另一个section）
        # 需要找到"基金单位市值"section
        row = 3
        in_unit_section = False

        while row < 1000:
            name_cell = self.ws.range(f'B{row}').value
            if name_cell and '基金单位市值' in str(name_cell):
                in_unit_section = True
                row += 1
                continue

            if in_unit_section:
                cell_code = self.ws.range(f'A{row}').value
                if cell_code is None or (isinstance(cell_code, str) and '市值' in cell_code):
                    break

                if str(cell_code).strip() == code:
                    self.ws.range((row, col_idx)).value = unit_price
                    self.logger.debug(f"更新 {code} 单位市值: {unit_price}")
                    break

            row += 1

    def save(self):
        """保存Excel文件"""
        try:
            # 创建备份
            import shutil
            backup_path = self.file_path.replace('.xlsx', f'.backup.{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx')
            try:
                shutil.copy2(self.file_path, backup_path)
                self.logger.info(f"已创建备份: {backup_path}")
            except Exception as e:
                self.logger.warning(f"创建备份失败: {e}")

            # 保存文件
            self.wb.save()
            self.logger.info(f"Excel文件已保存: {self.file_path}")

        except Exception as e:
            self.logger.error(f"保存失败: {e}")
            raise

    def close(self):
        """关闭工作簿和Excel应用"""
        try:
            self.wb.close()
            self.app.quit()
            self.logger.info("已关闭Excel")
        except Exception as e:
            self.logger.warning(f"关闭Excel时出错: {e}")
