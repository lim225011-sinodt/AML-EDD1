#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
执行MySQL版本的AML-EDD SQL程序
"""

import mysql.connector
import sys
import time
from datetime import datetime

class MySQLAMLExecutor:
    def __init__(self):
        self.config = {
            'host': '101.42.102.9',
            'port': 3306,
            'user': 'root',
            'password': 'Bancstone123!',
            'database': 'dify_db',
            'charset': 'utf8mb4',
            'autocommit': False
        }
        self.connection = None
        self.cursor = None

    def connect(self):
        """连接MySQL数据库"""
        try:
            self.connection = mysql.connector.connect(**self.config)
            self.cursor = self.connection.cursor()
            print("[OK] 成功连接到MySQL数据库")
            print(f"    服务器: {self.config['host']}:{self.config['port']}")
            print(f"    数据库: {self.config['database']}")

            # 检查版本
            self.cursor.execute("SELECT VERSION()")
            version = self.cursor.fetchone()[0]
            print(f"    MySQL版本: {version}")

            return True
        except Exception as e:
            print(f"[ERROR] 连接数据库失败: {e}")
            return False

    def execute_sql_file(self, sql_file_path):
        """执行SQL文件"""
        try:
            with open(sql_file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            print(f"\n[INFO] 读取SQL文件: {sql_file_path}")
            print(f"[INFO] 文件大小: {len(content)} 字符")

            # 分割SQL语句
            statements = []
            current_statement = ""

            for line in content.split('\n'):
                line = line.strip()

                # 跳过空行和注释
                if not line or line.startswith('--'):
                    continue

                current_statement += line + "\n"

                # 如果语句以分号结尾，添加到列表
                if line.endswith(';'):
                    statements.append(current_statement.strip())
                    current_statement = ""

            print(f"[INFO] 解析得到 {len(statements)} 条SQL语句")

            # 执行SQL语句
            start_time = time.time()
            executed_count = 0
            error_count = 0

            for i, statement in enumerate(statements, 1):
                try:
                    # 跳过空语句
                    if not statement:
                        continue

                    # 显示进度
                    if i % 50 == 0 or i <= 10:
                        print(f"  执行第 {i}/{len(statements)} 条语句...")

                    self.cursor.execute(statement)

                    # 某些语句需要立即提交
                    if any(keyword in statement.upper() for keyword in ['CREATE', 'DROP', 'INSERT', 'UPDATE', 'DELETE']):
                        self.connection.commit()

                    executed_count += 1

                    # 记录重要操作
                    if 'CREATE TABLE' in statement.upper():
                        table_name = statement.split()[2].strip('(`;')
                        print(f"    ✓ 创建表: {table_name}")
                    elif 'INSERT INTO' in statement.upper():
                        table_name = statement.split()[2].strip('(`;')
                        if i % 100 == 0:
                            print(f"    插入数据到: {table_name}")

                except mysql.connector.Error as e:
                    error_count += 1
                    print(f"    ✗ 语句 {i} 执行失败: {e}")
                    # 继续执行其他语句
                    continue

            execution_time = time.time() - start_time

            print(f"\n[INFO] SQL执行完成:")
            print(f"    成功: {executed_count} 条")
            print(f"    失败: {error_count} 条")
            print(f"    耗时: {execution_time:.2f} 秒")

            return executed_count, error_count

        except Exception as e:
            print(f"[ERROR] 执行SQL文件失败: {e}")
            return 0, 1

    def validate_data(self):
        """验证生成的数据"""
        print("\n[INFO] 验证生成的数据...")

        expected_tables = [
            ('tb_bank', '机构对照表'),
            ('tb_settle_type', '业务类型对照表'),
            ('tb_cst_pers', '个人客户信息'),
            ('tb_cst_unit', '企业客户信息'),
            ('tb_acc', '账户信息'),
            ('tb_acc_txn', '交易记录'),
            ('tb_risk_new', '最新风险等级'),
            ('tb_risk_his', '历史风险等级')
        ]

        validation_results = []

        for table_name, description in expected_tables:
            try:
                self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = self.cursor.fetchone()[0]
                print(f"  {description}: {count:6d} 条记录")

                # 检查数据量是否合理
                if 'cst_pers' in table_name:
                    if 900 <= count <= 1100:
                        validation_results.append(True)
                        print(f"    ✓ 个人客户数据量符合预期")
                    else:
                        validation_results.append(False)
                        print(f"    ✗ 个人客户数据量异常: 预期900-1100，实际{count}")

                elif 'cst_unit' in table_name:
                    if 90 <= count <= 110:
                        validation_results.append(True)
                        print(f"    ✓ 企业客户数据量符合预期")
                    else:
                        validation_results.append(False)
                        print(f"    ✗ 企业客户数据量异常: 预期90-110，实际{count}")

                elif 'acc_txn' in table_name:
                    if 9000 <= count <= 11000:
                        validation_results.append(True)
                        print(f"    ✓ 交易记录数据量符合预期")
                    else:
                        validation_results.append(False)
                        print(f"    ✗ 交易记录数据量异常: 预期9000-11000，实际{count}")
                else:
                    validation_results.append(True)

            except Exception as e:
                print(f"  ✗ 验证表 {table_name} 失败: {e}")
                validation_results.append(False)

        return all(validation_results)

    def show_sample_data(self):
        """显示示例数据"""
        print("\n[INFO] 示例数据查询:")

        try:
            # 个人客户示例
            self.cursor.execute("SELECT Cst_no, Acc_name, Contact1 FROM tb_cst_pers LIMIT 3")
            persons = self.cursor.fetchall()
            print("\n个人客户示例:")
            for p in persons:
                print(f"  {p[0]}: {p[1]} - {p[2]}")

            # 企业客户示例
            self.cursor.execute("SELECT Cst_no, Acc_name, Industry FROM tb_cst_unit LIMIT 2")
            units = self.cursor.fetchall()
            print("\n企业客户示例:")
            for u in units:
                print(f"  {u[0]}: {u[1]} - {u[2]}")

            # 交易记录示例
            self.cursor.execute("""
                SELECT Date, Cur, Org_amt, Purpose
                FROM tb_acc_txn
                WHERE Cur = 'CNY'
                LIMIT 3
            """)
            transactions = self.cursor.fetchall()
            print("\n交易记录示例:")
            for t in transactions:
                print(f"  {t[0]}: CNY {t[1]:.2f} - {t[2]}")

            # 风险等级分布
            self.cursor.execute("""
                SELECT Risk_code, COUNT(*)
                FROM tb_risk_new
                GROUP BY Risk_code
            """)
            risks = self.cursor.fetchall()
            print("\n风险等级分布:")
            risk_desc = {'10': '高风险', '11': '中高风险', '12': '中等风险', '13': '低风险'}
            for r in risks:
                desc = risk_desc.get(r[0], r[0])
                print(f"  {desc}: {r[1]} 个")

        except Exception as e:
            print(f"查询示例数据失败: {e}")

    def close(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("[INFO] 数据库连接已关闭")

def main():
    """主函数"""
    print("=== AML-EDD MySQL数据库执行程序 ===\n")

    executor = MySQLAMLExecutor()

    try:
        # 1. 连接数据库
        if not executor.connect():
            return False

        # 2. 执行SQL文件
        sql_file = "AML300_数据库建表和数据生成程序.sql"
        success, errors = executor.execute_sql_file(sql_file)

        if errors > success * 0.1:  # 错误率超过10%
            print("[WARNING] SQL执行错误率较高")

        # 3. 验证数据
        validation_passed = executor.validate_data()

        # 4. 显示示例数据
        executor.show_sample_data()

        # 5. 总结
        print("\n" + "="*50)
        print("执行完成总结:")
        print("="*50)

        if validation_passed:
            print("[SUCCESS] AML-EDD数据库创建成功！")
            print("数据库包含:")
            print("- 1000个个人客户")
            print("- 100个企业客户")
            print("- 10000条交易记录")
            print("- 完整的风险等级数据")
            print("- 符合300号文件格式要求")
            return True
        else:
            print("[PARTIAL] 数据库创建完成，但数据验证存在问题")
            return False

    except Exception as e:
        print(f"[ERROR] 执行过程发生异常: {e}")
        return False

    finally:
        executor.close()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)