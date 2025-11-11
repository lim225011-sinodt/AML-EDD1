#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查AML300数据库表结构
"""

import mysql.connector
import sys

def check_table_structures():
    """检查所有表的结构"""
    print("=== 检查AML300数据库表结构 ===\n")

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

        for table in tables:
            print(f"\n--- {table} 表结构 ---")
            cursor.execute(f"DESCRIBE {table}")
            columns = cursor.fetchall()
            for column in columns:
                field, type_, null, key, default, extra = column
                print(f"  {field:20} {type_:20} NULL: {null} KEY: {key}")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"检查失败: {e}")
        return False

if __name__ == "__main__":
    success = check_table_structures()
    sys.exit(0 if success else 1)