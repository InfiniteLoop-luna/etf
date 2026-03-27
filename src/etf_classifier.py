# -*- coding: utf-8 -*-
"""
ETF分类统计模块 - fetching from Tushare and processing logic
"""

import pandas as pd
import io
import logging
from typing import Dict
from src.volume_fetcher import _init_tushare

logger = logging.getLogger(__name__)

def fetch_etf_data() -> pd.DataFrame:
    """
    从 Tushare 获取全量 ETF 基本信息
    使用 etf_basic 接口
    """
    try:
        pro = _init_tushare()
        logger.info("开始从 Tushare 获取 ETF 基础信息 (etf_basic)...")
        # 获取基础信息，不指定 fields，让它返回所有可用字段
        df = pro.etf_basic()
        logger.info(f"成功获取 ETF 基础信息，共 {len(df)} 条")
        
        # 为了兼容并按照文档的列名操作，这里进行列名映射和补充
        # 实际 Tushare 字段可能为: ts_code, name, management, m_fee, c_fee, fund_type, invest_type, type, benchmark, status, etc.
        # 我们按照最可能的字段进行映射，其余如果有具体中文的直接用或者推断
        
        rename_map = {
            'ts_code': '基金交易代码',
            'name': 'ETF扩位简称',  # Tushare 的 name 通常是扩位简称
            'fullname': '基金中文全称', # 如果有 fullname
            'cnname': '基金中文全称',   # 另一个可能的中文名称段
            'idx_code': 'ETF基准指数代码', # 尝试几个可能的指数代码字段名
            'index_code': 'ETF基准指数代码',
            'benchmark_code': 'ETF基准指数代码',
            'benchmark': 'ETF基准指数中文全称',
            'status': '存续状态（L上市 D退市 P待上市）',
            'management': '基金管理人简称',
            'm_fee': '基金管理人收取的费用',
            # 如果有QDII或通道专门的标记字段，Tushare接口返回为 etf_type
            'etf_type': '基金投资通道类型（境内、QDII）',
            'fund_type': '基金投资通道类型（境内、QDII）', 
            'invest_type': '基金投资通道类型（境内、QDII）',
        }
        
        # 批量改名
        for col_en, col_cn in rename_map.items():
            if col_en in df.columns and col_cn not in df.columns:
                df = df.rename(columns={col_en: col_cn})
        
        # 补全可能缺失的列，确保后续 DataFrame 操作不报错
        required_cols = [
            '基金交易代码', 'ETF扩位简称', '基金中文全称', 'ETF基准指数代码', 
            'ETF基准指数中文全称', '存续状态（L上市 D退市 P待上市）', 
            '基金管理人简称', '基金管理人收取的费用', '基金投资通道类型（境内、QDII）'
        ]
        
        for col in required_cols:
            if col not in df.columns:
                df[col] = ''  # 填充为空字符串
                
        # 由于 Tushare 拿回来的 m_fee 是纯数字(比如0.5)，可以转成百分比字符串或者保留数字。此处保持原样。

        # 过滤掉基金交易代码以 .OF 或 .of 结尾的数据（通常为场外基金或联接基金等）
        if '基金交易代码' in df.columns:
            df = df[~df['基金交易代码'].astype(str).str.upper().str.endswith('.OF')].copy()

        return df

    except Exception as e:
        logger.error(f"获取 Tushare ETF 数据失败: {e}", exc_info=True)
        raise e

def process_etf_classification(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    根据文档步骤处理 ETF 数据，返回各表组合的字典
    """
    # 建立输出容器
    results = {}
    
    # ------------------- STEP 1 -------------------
    # 将下载的数据表命名为ETF汇总表
    df_summary = df.copy()
    
    # 删除ETF汇总表中存续状态为退市的数据 ('D' 退市)
    status_col = '存续状态（L上市 D退市 P待上市）'
    if status_col in df_summary.columns:
        # 剔除值为 'D' 或者包含 '退市' 的行
        df_summary = df_summary[
            ~df_summary[status_col].astype(str).str.contains('D|退市', case=False, na=False)
        ].copy()
        
    # ------------------- STEP 2 -------------------
    # 在ETF汇总表单独增加列，列名称为一级分类；
    df_summary['一级分类'] = ''
    
    # 为了防止一些列存在NaN，先填充好空字符串用于判断
    idx_code_col = 'ETF基准指数代码'
    df_summary[idx_code_col] = df_summary[idx_code_col].fillna('').astype(str).str.strip()
    # 同时如果 Tushare 没有给出正规的 idx_code（比如是 'None'），处理掉
    df_summary.loc[df_summary[idx_code_col].str.lower() == 'none', idx_code_col] = ''
    df_summary.loc[df_summary[idx_code_col] == 'nan', idx_code_col] = ''
    
    # 将ETF汇总表中ETF基准指数代码不为空值的数据单独汇总为一个表，命名为主要ETF分类表；
    df_main_etf = df_summary[df_summary[idx_code_col] != ''].copy()
    results['主要ETF分类表'] = df_main_etf
    
    # 将ETF汇总表中ETF基准指数代码为空值的数据单独汇总为一个表，命名为其他分类ETF表（待整理，应为货币ETF）
    # 整理后，将对应数据行的一级分类列赋值为货币；
    df_other_etf = df_summary[df_summary[idx_code_col] == ''].copy()
    df_other_etf['一级分类'] = '货币'
    df_summary.loc[df_summary[idx_code_col] == '', '一级分类'] = '货币'
    results['其他分类ETF表'] = df_other_etf
    
    # 将ETF分类表按照境内/QDII通道ETF拆分为两个表，分别命名为境内ETF表和QDII ETF表；
    channel_col = '基金投资通道类型（境内、QDII）'
    name_cols = ['ETF扩位简称', '基金中文全称', 'ETF基准指数中文全称', 'cnname']
    
    # 基于用户提示："有'基金投资通道类型'，用这个啊"，我们尝试优先提取 Tushare API mapped 后的列数据。
    # 由于该列在 Tushare 里面可能有各种值 (比如 "ETF"、"QDII")，我们统一标准化一下
    if channel_col in df_summary.columns:
        df_summary[channel_col] = df_summary[channel_col].fillna('').astype(str)
        # 如果原来列名是 QDII，我们就给它固定下来。如果没有明显包含 QDII，并且不是空的，就不动。
        # 这里进行一个强制规范化识别：如果列内容本身包含 QDII，或者基金名字包含 QDII 等，就属于 QDII
        
        qdii_mask = (df_summary[channel_col].str.contains('QDII', case=False, na=False))
        # 增加一些名称容错
        for ncol in name_cols:
            if ncol in df_summary.columns:
                df_summary[ncol] = df_summary[ncol].fillna('')
                qdii_mask = qdii_mask | df_summary[ncol].str.contains('QDII|纳斯达克|标普|日经|恒生|道琼斯', case=False, na=False)
                
        # 强制更新对应通道名称为 "QDII"
        df_summary.loc[qdii_mask, channel_col] = 'QDII'
        # 未被归类为 QDII 的所有基金，预设为境内
        df_summary.loc[~qdii_mask, channel_col] = '境内'
        
    df_qdii = df_summary[df_summary[channel_col] == 'QDII'].copy()
    df_domestic = df_summary[df_summary[channel_col] == '境内'].copy()
    
    # 将QDII ETF表的一级分类列赋值QDII；并同步到 Summary 中已确认为 QDII 且没赋为"货币"的数据
    df_qdii['一级分类'] = 'QDII'
    
    # 同步汇总表中
    df_summary.loc[(df_summary[channel_col] == 'QDII') & (df_summary['一级分类'] == ''), '一级分类'] = 'QDII'
    results['QDII ETF表'] = df_qdii
    results['境内ETF表'] = df_domestic

    # ------------------- STEP 3 -------------------
    # 将境内ETF表中所有ETF基准指数代码数据汇总成单独的数据表，命名为境内ETF基准指数表；
    # 就是根据基准指数代码去重
    df_domestic_idx = df_domestic.drop_duplicates(subset=[idx_code_col]).copy()
    if '' in df_domestic_idx[idx_code_col].values:
        df_domestic_idx = df_domestic_idx[df_domestic_idx[idx_code_col] != '']

    results['境内ETF基准指数表'] = df_domestic_idx
    
    # 在境内ETF基准指数代码表中，筛选ETF基准指数中文全称中的商品/债的关键字；
    # 根据取值，将ETF汇总表对应境内ETF基准指数代码数据行的一级分类列赋值为商品/债券；
    # 相比于仅用代码映射，我们直接利用名字本身在汇总表里强搜，防止因为 Tushare 接口偶尔漏掉指数代码导致分类失败
    
    is_commodity = pd.Series([False]*len(df_summary), index=df_summary.index)
    is_bond = pd.Series([False]*len(df_summary), index=df_summary.index)
    
    for ncol in ['ETF扩位简称', '基金中文全称', 'ETF基准指数中文全称', 'cnname']:
        if ncol in df_summary.columns:
            col_data = df_summary[ncol].fillna('')
            is_commodity = is_commodity | col_data.str.contains('商品|黄金|有色金属|商品指数', na=False)
            is_bond = is_bond | col_data.str.contains('债', na=False)
            
    is_domestic = df_summary[channel_col] == '境内'
    
    df_summary.loc[is_domestic & is_commodity & (df_summary['一级分类'] == ''), '一级分类'] = '商品'
    df_summary.loc[is_domestic & is_bond & (df_summary['一级分类'] == ''), '一级分类'] = '债券'
    
    # 剩下的一级分类为空的境内基金均赋值为指数
    df_summary.loc[is_domestic & (df_summary['一级分类'] == ''), '一级分类'] = '指数'

    
    # ------------------- STEP 4 -------------------
    # 在ETF汇总表单独增加列，列名称为二级分类；
    df_summary['二级分类'] = ''
    
    # 在ETF汇总表中一级分类列赋值为指数的数据行筛选关键字增强，并在对应的二级分类列赋值为增强；
    # 这里关键字增强可以查基金中文全称或者简称
    is_index = df_summary['一级分类'] == '指数'
    
    # 查找“增强”
    name_search_mask = pd.Series([False] * len(df_summary), index=df_summary.index)
    for ncol in ['ETF扩位简称', '基金中文全称', 'ETF基准指数中文全称']:
        if ncol in df_summary.columns:
            name_search_mask = name_search_mask | df_summary[ncol].fillna('').str.contains('增强', na=False)
            
    enhanced_mask = is_index & name_search_mask
    df_summary.loc[enhanced_mask, '二级分类'] = '增强'
    
    # 将ETF汇总表中一级分类列赋值为指数的其他数据导出，命名为指数ETF表，按ETF基准指数代码做汇总；
    # 这里的“其他数据”是指二级分类尚为空的（不是“增强”）的指数ETF
    index_other_mask = is_index & (df_summary['二级分类'] == '')
    df_index_etf = df_summary[index_other_mask].copy()
    
    # 手工在二级分类录入宽基；(此处程序预留或者置空)
    # 在指数ETF表中其他二级分类未赋值的数据行中赋值行业&其他；
    df_index_etf['二级分类'] = '行业&其他'
    # 同步回汇总表，没填且是指数的给 "行业&其他"，用户如果想填"宽基"可以自己在下载的表里改
    df_summary.loc[index_other_mask, '二级分类'] = '行业&其他'
    
    # 根据ETF基准指数代码对指数ETF表中数据行进行排序；
    df_index_etf = df_index_etf.sort_values(by=[idx_code_col])
    results['指数ETF表'] = df_index_etf
    
    # 为了保证最终汇总表是最完整的，我们重新放回结果里
    results['ETF汇总表'] = df_summary
    
    # 此时还需要让 `境内ETF表`, `QDII ETF表`, `其他分类ETF表`,  等表保持和汇总表更新后的一级、二级分类同步。
    # 这里我们采用重新从汇总表中按照规则提取最新的数据覆盖，以保持数据完全一致。
    results['境内ETF表'] = df_summary[df_summary[channel_col] == '境内'].copy()
    results['QDII ETF表'] = df_summary[df_summary[channel_col] == 'QDII'].copy()
    results['其他分类ETF表'] = df_summary[df_summary[idx_code_col] == ''].copy()
    
    return results


def export_etfs_to_excel(results: Dict[str, pd.DataFrame]) -> bytes:
    """
    将处理好的多个 DataFrame 导出为带有 Sheet 形式的单一 Excel 文件
    返回文件字节流供 Streamlit 下载
    """
    output = io.BytesIO()
    
    # 文档里提到的 Sheet 顺序
    sheet_order = [
        'ETF汇总表', '主要ETF分类表', '其他分类ETF表', 
        '境内ETF表', 'QDII ETF表', '境内ETF基准指数表', 
        '指数ETF表'
    ]
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for sheet_name in sheet_order:
            if sheet_name in results:
                df_to_save = results[sheet_name]
                # 重新调整序号等，不保留 DataFrame 源索引
                df_to_save.to_excel(writer, sheet_name=sheet_name, index=False)
                
    return output.getvalue()
