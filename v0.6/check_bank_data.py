#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查银行数据
"""

import mysql.connector
import sys

def check_bank_data():
    """检查银行数据"""
    print("=== 检查银行数据 ===\n")

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

        # 查看银行数据
        cursor.execute("SELECT * FROM tb_bank")
        banks = cursor.fetchall()

        print("银行表数据:")
        for bank in banks:
            print(f"  {bank}")

        # 查看业务类型数据
        cursor.execute("SELECT * FROM tb_settle_type")
        settle_types = cursor.fetchall()

        print("\n业务类型表数据:")
        for settle_type in settle_types:
            print(f"  {settle_type}")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"[ERROR] 检查失败: {e}")
        return False

if __name__ == "__main__":
    success = check_bank_data()
    sys.exit(0 if success else 1)