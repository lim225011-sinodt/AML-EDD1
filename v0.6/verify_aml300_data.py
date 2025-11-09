#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证AML300数据库
"""

import mysql.connector
import sys

def verify_aml300_database():
    """验证AML300数据库"""
    print("=== AML300 数据库验证 ===\n")

    try:
        # 连接数据库
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

        # 检查表
        tables_to_check = [
            ('tb_bank', '机构对照表'),
            ('tb_settle_type', '业务类型对照表'),
            ('tb_cst_pers', '个人客户信息'),
            ('tb_cst_unit', '企业客户信息'),
            ('tb_acc', '账户信息'),
            ('tb_acc_txn', '交易记录'),
            ('tb_risk_new', '最新风险等级'),
            ('tb_risk_his', '历史风险等级')
        ]

        total_records = 0

        print("\n数据表统计:")
        print("=" * 50)
        for table_name, description in tables_to_check:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                total_records += count
                print(f"{description:15} : {count:8d} 条记录")
            except Exception as e:
                print(f"{description:15} : 查询失败 - {str(e)[:50]}")

        print("=" * 50)
        print(f"总记录数: {total_records:,}")

        # 查询示例数据
        print("\n示例数据查询:")
        print("-" * 30)

        # 个人客户
        try:
            cursor.execute("SELECT Cst_no, Acc_name, Contact1 FROM tb_cst_pers LIMIT 3")
            print("\n个人客户:")
            for row in cursor.fetchall():
                print(f"  {row[0]} - {row[1]} - {row[2]}")
        except:
            print("无法查询个人客户数据")

        # 企业客户
        try:
            cursor.execute("SELECT Cst_no, Acc_name, Industry FROM tb_cst_unit LIMIT 2")
            print("\n企业客户:")
            for row in cursor.fetchall():
                print(f"  {row[0]} - {row[1]} - {row[2]}")
        except:
            print("无法查询企业客户数据")

        # 交易记录
        try:
            cursor.execute("SELECT Date, Cur, Org_amt, Purpose FROM tb_acc_txn LIMIT 3")
            print("\n交易记录:")
            for row in cursor.fetchall():
                print(f"  {row[0]} - {row[1]} {row[2]:.2f} - {row[3]}")
        except:
            print("无法查询交易记录")

        # 风险等级分布
        try:
            cursor.execute("""
                SELECT Risk_code, COUNT(*)
                FROM tb_risk_new
                GROUP BY Risk_code
                ORDER BY Risk_code
            """)
            print("\n风险等级分布:")
            for row in cursor.fetchall():
                risk_desc = {'10': '高风险', '11': '中高风险', '12': '中等风险', '13': '低风险'}
                desc = risk_desc.get(row[0], row[0])
                print(f"  {desc}: {row[1]} 个客户")
        except:
            print("无法查询风险等级数据")

        # 验证数据质量
        print("\n数据质量验证:")
        print("-" * 20)

        # 验证个人客户数据量
        try:
            cursor.execute("SELECT COUNT(*) FROM tb_cst_pers")
            person_count = cursor.fetchone()[0]
            if 900 <= person_count <= 1100:
                print(f"[OK] 个人客户数据量正常: {person_count}")
            else:
                print(f"[WARNING] 个人客户数据量异常: {person_count}")
        except:
            print("[ERROR] 无法验证个人客户数据量")

        # 验证企业客户数据量
        try:
            cursor.execute("SELECT COUNT(*) FROM tb_cst_unit")
            unit_count = cursor.fetchone()[0]
            if 90 <= unit_count <= 110:
                print(f"[OK] 企业客户数据量正常: {unit_count}")
            else:
                print(f"[WARNING] 企业客户数据量异常: {unit_count}")
        except:
            print("[ERROR] 无法验证企业客户数据量")

        # 验证交易记录数据量
        try:
            cursor.execute("SELECT COUNT(*) FROM tb_acc_txn")
            txn_count = cursor.fetchone()[0]
            if 9000 <= txn_count <= 11000:
                print(f"[OK] 交易记录数据量正常: {txn_count}")
            else:
                print(f"[WARNING] 交易记录数据量异常: {txn_count}")
        except:
            print("[ERROR] 无法验证交易记录数据量")

        cursor.close()
        conn.close()

        print(f"\n验证完成! 数据库 AML300 共包含 {total_records:,} 条记录")
        return True

    except Exception as e:
        print(f"验证失败: {e}")
        return False

if __name__ == "__main__":
    success = verify_aml300_database()
    sys.exit(0 if success else 1)