#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查AML300数据库完整性（15张表）
"""

import mysql.connector
import sys

def check_aml300_complete():
    """检查AML300数据库是否包含完整的15张表"""
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
        all_tables = [table[0] for table in cursor.fetchall()]

        print(f"当前表数量: {len(all_tables)}")
        print("\n表列表:")
        for table in all_tables:
            print(f"  - {table}")

        # 期望的15张表
        expected_tables = {
            'tb_bank': '机构对照表',
            'tb_settle_type': '业务类型对照表',
            'tb_cst_pers': '个人客户信息表',
            'tb_cst_unit': '企业客户信息表',
            'tb_acc': '账户主档表',
            'tb_acc_txn': '非贷记账户交易表',
            'tb_risk_his': '历次风险等级划分表',
            'tb_risk_new': '最新风险等级表',
            'tb_cred_txn': '贷记账户交易表',
            'tb_cash_remit': '现金汇款/无卡无折现金存款表',
            'tb_cash_convert': '现钞结售汇/兑换明细表',
            'tb_cross_border': '跨境汇款交易表',
            'tb_lwhc_log': '公民联网核查日志表',
            'tb_lar_report': '大额交易报告明细表',
            'tb_sus_report': '可疑交易报告明细表'
        }

        print(f"\n期望的15张表:")
        missing_tables = []
        existing_tables = []

        for table_name, description in expected_tables.items():
            if table_name in all_tables:
                existing_tables.append((table_name, description))
                print(f"  OK {table_name}: {description}")
            else:
                missing_tables.append((table_name, description))
                print(f"  MISSING {table_name}: {description}")

        print(f"\n统计结果:")
        print(f"  已创建: {len(existing_tables)} 张表")
        print(f"  缺失: {len(missing_tables)} 张表")

        # 检查表记录数
        print(f"\n各表记录数统计:")
        total_records = 0
        for table_name, description in expected_tables.items():
            if table_name in all_tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    total_records += count
                    print(f"  {table_name:15} : {description:25} - {count:8d} 条记录")
                except Exception as e:
                    print(f"  {table_name:15} : {description:25} - 查询失败: {str(e)[:30]}")

        print(f"\n总记录数: {total_records:,}")

        if len(existing_tables) == 15:
            print(f"\nSUCCESS: AML300数据库已包含完整的15张表结构！")
            success = True
        else:
            print(f"\nWARNING: AML300数据库还缺少 {len(missing_tables)} 张表")
            success = False

        cursor.close()
        conn.close()

        return success

    except Exception as e:
        print(f"[ERROR] 检查失败: {e}")
        return False

if __name__ == "__main__":
    success = check_aml300_complete()
    sys.exit(0 if success else 1)