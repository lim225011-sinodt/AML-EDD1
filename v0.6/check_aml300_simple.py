#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查AML300数据库完整性
"""

import mysql.connector
import sys

def check_aml300():
    """检查AML300数据库"""
    print("=== 检查AML300数据库完整性 ===\n")

    try:
        conn = mysql.connector.connect(
            host='101.42.102.9',
            port=3306,
            user='root',
            password='Bancstone123!',
            database='AML300',
            charset='utf8mb4'
        )

        cursor = conn.cursor()
        print("成功连接到 AML300 数据库")

        # 获取所有表
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]

        print(f"当前表数量: {len(tables)}")
        print("\n表列表:")
        for table in tables:
            print(f"  - {table}")

        # 期望的15张表
        expected = [
            'tb_bank', 'tb_settle_type', 'tb_cst_pers', 'tb_cst_unit', 'tb_acc',
            'tb_acc_txn', 'tb_risk_his', 'tb_risk_new', 'tb_cred_txn',
            'tb_cash_remit', 'tb_cash_convert', 'tb_cross_border',
            'tb_lwhc_log', 'tb_lar_report', 'tb_sus_report'
        ]

        missing = [t for t in expected if t not in tables]

        print(f"\n缺失的表: {len(missing)}")
        for t in missing:
            print(f"  - {t}")

        if len(tables) == 15:
            print("\n[SUCCESS] AML300包含完整的15张表!")
        else:
            print(f"\n[PARTIAL] 包含 {len(tables)} 张表，缺少 {len(missing)} 张")

        # 显示记录数
        print("\n各表记录数:")
        total = 0
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                total += count
                print(f"  {table:15} : {count:8d}")
            except:
                print(f"  {table:15} : 查询失败")

        print(f"\n总计: {total:,} 条记录")

        cursor.close()
        conn.close()
        return len(tables) == 15

    except Exception as e:
        print(f"检查失败: {e}")
        return False

if __name__ == "__main__":
    success = check_aml300()
    sys.exit(0 if success else 1)