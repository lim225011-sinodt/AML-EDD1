#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化版MySQL AML-EDD执行程序
"""

import mysql.connector
import sys
import time

def execute_mysql_sql():
    """执行MySQL SQL程序"""
    print("=== MySQL AML-EDD 数据库执行 ===\n")

    # 连接配置
    config = {
        'host': '101.42.102.9',
        'port': 3306,
        'user': 'root',
        'password': 'Bancstone123!',
        'database': 'dify_db',
        'charset': 'utf8mb4',
        'autocommit': False
    }

    try:
        print("连接MySQL数据库...")
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        print("连接成功!")

        # 读取SQL文件
        sql_file = "AML300_数据库建表和数据生成程序.sql"
        print(f"读取SQL文件: {sql_file}")

        with open(sql_file, 'r', encoding='utf-8') as f:
            content = f.read()

        # 分割SQL语句
        statements = []
        current_statement = ""

        for line in content.split('\n'):
            line = line.strip()
            if not line or line.startswith('--'):
                continue

            current_statement += line + " "

            if line.endswith(';'):
                statements.append(current_statement.strip())
                current_statement = ""

        print(f"解析得到 {len(statements)} 条SQL语句")

        # 执行SQL语句
        start_time = time.time()
        success_count = 0
        error_count = 0

        # 先创建数据库（如果不存在）
        try:
            cursor.execute("CREATE DATABASE IF NOT EXISTS AML300 CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            cursor.execute("USE AML300")
            conn.commit()
            print("创建并切换到 AML300 数据库")
        except Exception as e:
            print(f"创建数据库警告: {e}")

        for i, statement in enumerate(statements, 1):
            try:
                # 跳过空语句
                if not statement or len(statement.strip()) < 10:
                    continue

                # 执行语句
                cursor.execute(statement)

                # 某些语句需要提交
                if any(keyword in statement.upper() for keyword in ['CREATE', 'DROP', 'INSERT', 'UPDATE', 'DELETE', 'ALTER']):
                    conn.commit()

                success_count += 1

                # 显示重要进度
                if i % 50 == 0:
                    print(f"已执行 {i}/{len(statements)} 条语句，成功 {success_count}")

                # 记录重要操作
                if 'CREATE TABLE' in statement.upper():
                    parts = statement.split()
                    if len(parts) >= 3:
                        table_name = parts[2].strip('(`;')
                        print(f"创建表: {table_name}")

                elif 'INSERT INTO' in statement.upper() and i % 500 == 0:
                    parts = statement.split()
                    if len(parts) >= 3:
                        table_name = parts[2].strip('(`;')
                        print(f"插入数据: {table_name}")

            except mysql.connector.Error as e:
                error_count += 1
                if error_count <= 10:  # 只显示前10个错误
                    print(f"语句 {i} 错误: {str(e)[:100]}")
                continue

        execution_time = time.time() - start_time

        print(f"\n执行完成:")
        print(f"成功: {success_count} 条")
        print(f"失败: {error_count} 条")
        print(f"耗时: {execution_time:.2f} 秒")

        # 验证数据
        print("\n验证生成的数据...")
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

        for table_name, description in tables_to_check:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                print(f"{description}: {count:8d} 条记录")
            except Exception as e:
                print(f"{description}: 查询失败 - {str(e)[:50]}")

        # 查询示例数据
        print("\n示例数据:")
        try:
            cursor.execute("SELECT Cst_no, Acc_name FROM tb_cst_pers LIMIT 3")
            for row in cursor.fetchall():
                print(f"客户: {row[0]} - {row[1]}")
        except:
            print("无法查询客户数据")

        cursor.close()
        conn.close()

        print("\nMySQL数据库程序执行完成!")
        return True

    except Exception as e:
        print(f"执行失败: {e}")
        return False

if __name__ == "__main__":
    success = execute_mysql_sql()
    sys.exit(0 if success else 1)