"""测试数据加载器"""

import logging
from src.data_loader import load_etf_data

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    print("=" * 60)
    print("测试数据加载器")
    print("=" * 60)

    # 加载数据
    df = load_etf_data('主要ETF基金份额变动情况.xlsx')

    print("\n数据加载完成！")
    print(f"总行数: {len(df)}")
    print(f"\n数据列: {list(df.columns)}")

    print("\n数据类型:")
    print(df.dtypes)

    print("\n前10行数据:")
    print(df.head(10))

    print("\n数据统计:")
    print(f"唯一ETF代码数: {df[~df['is_aggregate']]['code'].nunique()}")
    print(f"唯一日期数: {df['date'].nunique()}")
    print(f"唯一指标类型数: {df['metric_type'].nunique()}")
    print(f"汇总行数: {df['is_aggregate'].sum()}")
    print(f"非汇总行数: {(~df['is_aggregate']).sum()}")

    if df['is_aggregate'].sum() > 0:
        print("\n汇总行示例:")
        print(df[df['is_aggregate']].head(10).to_string())

    print("\n指标类型列表:")
    for metric in df['metric_type'].unique():
        count = len(df[df['metric_type'] == metric])
        print(f"  - {metric}: {count} 行")

    print("\nETF列表 (前10个):")
    etf_codes = df[~df['is_aggregate']]['code'].unique()
    for code in etf_codes[:10]:
        name = df[df['code'] == code]['name'].iloc[0]
        print(f"  - {code}: {name}")

    print("\n日期范围:")
    print(f"  最早: {df['date'].min()}")
    print(f"  最晚: {df['date'].max()}")

    print("\n示例数据 (某个ETF的所有记录):")
    if len(etf_codes) > 0:
        sample_code = etf_codes[0]
        sample_data = df[df['code'] == sample_code].head(20)
        print(sample_data.to_string())

    print("\n" + "=" * 60)
    print("测试完成！")
    print("=" * 60)


if __name__ == '__main__':
    main()
