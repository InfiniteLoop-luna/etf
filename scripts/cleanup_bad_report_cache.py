#!/usr/bin/env python3
"""
清理数据库中包含'无K线数据'的错误缓存报告。
在服务器上运行: python scripts/cleanup_bad_report_cache.py
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from src.security_intraday_store import get_engine

def main():
    print("=" * 50)
    print("  清理错误报告缓存")
    print("=" * 50)
    
    try:
        engine = get_engine()
    except Exception as e:
        print(f"❌ 数据库连接失败: {e}")
        return
    
    # 1. 查看当前缓存状态
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT ts_code, trade_date, LENGTH(report_md) as len, "
            "LEFT(report_md, 200) as preview, created_at "
            "FROM ts_distribution_reports ORDER BY created_at DESC"
        ))
        rows = result.fetchall()
        print(f"\n📋 当前缓存报告: {len(rows)} 条\n")
        for row in rows:
            is_bad = "无K线数据" in (row[3] or "")
            status = "❌ 错误" if is_bad else "✅ 正常"
            print(f"  {row[0]} | {row[1]} | {row[2]}字符 | {status} | {row[4]}")
    
    # 2. 删除包含"无K线数据"的错误缓存
    with engine.begin() as conn:
        result = conn.execute(text(
            "DELETE FROM ts_distribution_reports WHERE report_md LIKE '%无K线数据%'"
        ))
        deleted = result.rowcount
        print(f"\n🗑️ 已删除 {deleted} 条错误缓存")
    
    # 3. 测试 mootdx 连接
    print("\n" + "=" * 50)
    print("  测试 mootdx 连接")
    print("=" * 50)
    
    try:
        from src.distribution_analyzer import create_client
        client = create_client()
        test = client.bars(symbol='000733', frequency=9, offset=5)
        if test is not None and not test.empty:
            print(f"✅ mootdx 连接正常, 获取到 {len(test)} 条K线")
            print(test.tail(3).to_string())
        else:
            print("❌ mootdx 连接成功但返回空数据")
        client.close()
    except Exception as e:
        print(f"❌ mootdx 连接失败: {e}")
    
    print("\n✅ 清理完成! 现在可以重新点击按钮生成报告了。")

if __name__ == "__main__":
    main()
