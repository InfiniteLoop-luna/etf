from src.data_loader import load_etf_data

print("正在加载数据...")
df = load_etf_data('主要ETF基金份额变动情况.xlsx')

print(f"\n总数据行数: {len(df)}")
print(f"\n检测到的指标类型:")
for metric in df['metric_type'].unique():
    count = len(df[df['metric_type'] == metric])
    print(f"  - {metric}: {count} 条记录")

print(f"\n数据列: {df.columns.tolist()}")
print(f"\n前5行数据:")
print(df.head())
