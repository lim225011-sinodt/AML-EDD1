#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySQL远程连接测试程序
"""

import mysql.connector
import sys

def test_mysql_connection():
    """测试MySQL远程连接"""
    print("测试MySQL远程连接...")

    try:
        # 使用您提供的配置
        conn = mysql.connector.connect(
            host='101.42.102.9',
            port=3306,
            user='root',
            password='Bancstone123!',
            database='dify_db',
            charset='utf8mb4'
        )

        cursor = conn.cursor()

        # 测试基本查询
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()[0]
        print(f"MySQL版本: {version}")

        # 测试字符集
        cursor.execute("SHOW VARIABLES LIKE 'character_set_server'")
        charset = cursor.fetchone()[1]
        print(f"服务器字符集: {charset}")

        # 测试数据库
        cursor.execute("SELECT DATABASE()")
        database = cursor.fetchone()[0]
        print(f"当前数据库: {database}")

        # 测试创建表权限
        cursor.execute("SHOW TABLES LIMIT 5")
        tables = cursor.fetchall()
        print(f"现有表数量: {len(tables)}")

        cursor.close()
        conn.close()

        print("[OK] MySQL连接测试成功！")
        return True

    except Exception as e:
        print(f"[ERROR] MySQL连接失败: {e}")
        return False

def test_sql_execution():
    """测试SQL执行"""
    print("\n测试SQL执行...")

    try:
        conn = mysql.connector.connect(
            host='101.42.102.9',
            port=3306,
            user='root',
            password='Bancstone123!',
            database='dify_db',
            charset='utf8mb4'
        )

        cursor = conn.cursor()

        # 测试创建临时表
        cursor.execute("""
            CREATE TEMPORARY TABLE IF NOT EXISTS test_table (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)

        # 测试插入数据
        cursor.execute("INSERT INTO test_table (id, name) VALUES (1, '测试数据'), (2, '中文测试')")

        # 测试查询
        cursor.execute("SELECT * FROM test_table")
        results = cursor.fetchall()
        print(f"测试数据: {results}")

        # 清理
        cursor.execute("DROP TEMPORARY TABLE IF EXISTS test_table")

        conn.commit()
        cursor.close()
        conn.close()

        print("[OK] SQL执行测试成功！")
        return True

    except Exception as e:
        print(f"[ERROR] SQL执行失败: {e}")
        return False

if __name__ == "__main__":
    print("=== MySQL远程连接和SQL执行测试 ===\n")

    success1 = test_mysql_connection()
    success2 = test_sql_execution()

    if success1 and success2:
        print("\n[SUCCESS] 所有测试通过！可以执行完整的SQL程序。")
        sys.exit(0)
    else:
        print("\n[FAIL] 测试失败，请检查连接配置。")
        sys.exit(1)