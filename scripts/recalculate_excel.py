"""
重新计算 Excel 文件中的所有公式

这个脚本使用 xlwings 库来打开 Excel 文件，强制重新计算所有公式，然后保存。
需要：
1. Windows 操作系统
2. 安装 Microsoft Excel
3. 安装 xlwings: pip install xlwings

使用方法：
    python scripts/recalculate_excel.py
"""

import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import xlwings as xw
except ImportError:
    print("错误：未安装 xlwings 库")
    print("请运行: pip install xlwings")
    sys.exit(1)

def recalculate_excel(file_path: str):
    """使用 Excel 重新计算文件中的所有公式"""
    print(f"正在打开文件: {file_path}")

    # 打开 Excel 应用（不可见）
    app = xw.App(visible=False)

    try:
        # 打开工作簿
        wb = app.books.open(file_path)

        print("正在重新计算所有公式...")

        # 强制完全重新计算
        wb.app.calculate()

        # 保存文件
        print("正在保存文件...")
        wb.save()

        print("✓ 完成！所有公式已重新计算并保存")

    finally:
        # 关闭工作簿和应用
        wb.close()
        app.quit()

if __name__ == '__main__':
    file_path = '主要ETF基金份额变动情况.xlsx'

    if not os.path.exists(file_path):
        print(f"错误：文件不存在: {file_path}")
        sys.exit(1)

    recalculate_excel(file_path)
