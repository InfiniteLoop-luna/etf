"""快速开始脚本 - 一键创建数据库并导入数据"""

import os
import sys

def main():
    print("=" * 60)
    print("ETF数据库快速开始")
    print("=" * 60)
    print()

    # 检查Excel文件是否存在
    excel_file = '../主要ETF基金份额变动情况.xlsx'
    if not os.path.exists(excel_file):
        print(f"❌ 错误: 找不到Excel文件 {excel_file}")
        print("请确保Excel文件在项目根目录下")
        return

    print("✅ 找到Excel文件")
    print()

    # 选择数据库类型
    print("请选择数据库类型:")
    print("1. SQLite（推荐用于本地开发）")
    print("2. PostgreSQL（推荐用于生产环境）")
    print()

    choice = input("请输入选项 (1 或 2): ").strip()

    if choice == '1':
        # SQLite
        print()
        print("=" * 60)
        print("使用SQLite数据库")
        print("=" * 60)
        print()

        from import_data import import_to_sqlite

        db_path = input("数据库文件路径 (默认: etf_data.db): ").strip()
        if not db_path:
            db_path = 'etf_data.db'

        print()
        print(f"开始导入数据到 {db_path}...")
        print()

        try:
            stats = import_to_sqlite(excel_file, db_path)

            print()
            print("=" * 60)
            print("✅ 导入完成!")
            print("=" * 60)
            print(f"  新增记录: {stats['inserted']} 条")
            print(f"  更新记录: {stats['updated']} 条")
            print(f"  失败记录: {stats['failed']} 条")
            print()
            print("下一步:")
            print("1. 设置环境变量:")
            print("   Windows: set DATA_SOURCE=database && set DB_TYPE=sqlite && set DB_PATH=" + db_path)
            print("   Linux/Mac: export DATA_SOURCE=database && export DB_TYPE=sqlite && export DB_PATH=" + db_path)
            print()
            print("2. 运行Streamlit应用:")
            print("   streamlit run ../app_with_db.py")
            print()

        except Exception as e:
            print(f"❌ 导入失败: {e}")
            import traceback
            traceback.print_exc()

    elif choice == '2':
        # PostgreSQL
        print()
        print("=" * 60)
        print("使用PostgreSQL数据库")
        print("=" * 60)
        print()

        print("请输入PostgreSQL连接信息:")
        host = input("主机 (默认: localhost): ").strip() or 'localhost'
        port = input("端口 (默认: 5432): ").strip() or '5432'
        database = input("数据库名 (默认: etf_data): ").strip() or 'etf_data'
        username = input("用户名: ").strip()
        password = input("密码: ").strip()

        connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"

        print()
        print("开始导入数据...")
        print()

        try:
            from import_data import import_to_postgresql

            stats = import_to_postgresql(excel_file, connection_string)

            print()
            print("=" * 60)
            print("✅ 导入完成!")
            print("=" * 60)
            print(f"  新增记录: {stats['inserted']} 条")
            print(f"  更新记录: {stats['updated']} 条")
            print(f"  失败记录: {stats['failed']} 条")
            print()
            print("下一步:")
            print("1. 设置环境变量:")
            print("   Windows: set DATA_SOURCE=database && set DB_TYPE=postgresql")
            print("   Linux/Mac: export DATA_SOURCE=database && export DB_TYPE=postgresql")
            print()
            print("2. 配置Streamlit secrets (.streamlit/secrets.toml):")
            print(f'   DATABASE_URL = "{connection_string}"')
            print()
            print("3. 运行Streamlit应用:")
            print("   streamlit run ../app_with_db.py")
            print()

        except ImportError:
            print("❌ 错误: 未安装psycopg2")
            print("请运行: pip install psycopg2-binary")
        except Exception as e:
            print(f"❌ 导入失败: {e}")
            import traceback
            traceback.print_exc()

    else:
        print("❌ 无效的选项")


if __name__ == '__main__':
    main()
