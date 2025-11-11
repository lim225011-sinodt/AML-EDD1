#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
逐步插入数据，避免参数问题
严格按照300号文要求：总行下属各分行，2010-2025年开户
"""

import mysql.connector
import random
from datetime import datetime, timedelta
import sys

def step_by_step_data():
    """逐步插入数据"""
    print("=== 逐步插入AML300数据 ===")
    print("严格按照300号文要求")

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

        # 获取银行信息
        cursor.execute("SELECT Bank_code1, Bank_name FROM tb_bank")
        banks = cursor.fetchall()
        print(f"银行分行信息: {len(banks)} 个")

        # 清理现有数据
        print("\n清理现有数据...")
        tables = ['tb_lar_report', 'tb_sus_report', 'tb_lwhc_log', 'tb_cross_border',
                 'tb_cash_convert', 'tb_cash_remit', 'tb_cred_txn', 'tb_acc_txn',
                 'tb_risk_his', 'tb_risk_new', 'tb_acc', 'tb_cst_unit', 'tb_cst_pers']

        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        for table in tables:
            cursor.execute(f"DELETE FROM {table}")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        print("数据清理完成")

        # 第1步：插入1个个人客户测试
        print("\n第1步：插入个人客户测试...")
        start_date = datetime(2010, 1, 1)
        end_date = datetime(2025, 1, 1)
        random_days = random.randint(0, (end_date - start_date).days)
        open_date = (start_date + timedelta(days=random_days)).strftime('%Y%m%d')

        sql1 = """INSERT INTO tb_cst_pers (Head_no, Bank_code1, Cst_no, Open_time, Close_time, Acc_name,
                                        Cst_sex, Nation, Id_type, Id_no, Id_deadline, Occupation, Income,
                                        Contact1, Contact2, Contact3, Address1, Address2, Address3, Company, Sys_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        pers_data = (
            "ABC001",
            banks[0][0],  # 使用第一个银行分行
            "P000001",
            open_date,
            None,
            "客户1",
            "11",
            "CHN",
            "11",
            "1101011990010101234",
            "20300101",
            "软件工程师",
            120000.00,
            "13800000001",
            None, None,
            "北京市朝阳区1号",
            None, None,
            "公司1",
            "系统001"
        )
        cursor.execute(sql1, pers_data)
        print("个人客户1插入成功")

        # 第2步：插入1个企业客户测试
        print("\n第2步：插入企业客户测试...")
        sql2 = """INSERT INTO tb_cst_unit (Head_no, Bank_code1, Cst_no, Open_time, Acc_name, Rep_name,
                                        Ope_name, License, Id_deadline, Industry, Reg_amt, Reg_amt_code, Sys_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        unit_data = (
            "ABC001",
            banks[1][0],  # 使用第二个银行分行
            "U000001",
            open_date,
            "企业1科技有限公司",
            "法人1",
            "经办人1",
            "LICENSE20000000001",
            "20300101",
            "软件开发",
            10000000.00,
            "CNY",
            "企业系统001"
        )
        cursor.execute(sql2, unit_data)
        print("企业客户1插入成功")

        # 第3步：插入账户测试
        print("\n第3步：插入账户测试...")
        sql3 = """INSERT INTO tb_acc (Head_no, Bank_code1, Self_acc_name, Acc_state, Self_acc_no, Card_no,
                                    Acc_type, Acc_type1, Id_no, Cst_no, Open_time, Close_time,
                                    Agency_flag, Acc_flag, Fixed_flag)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        # 个人账户
        acc_data1 = (
            "ABC001",
            banks[0][0],
            "客户1",
            "11",
            "62284804012345678901",
            "62254804012345678901",
            "11",
            "21",
            "1101011990010101234",
            "P000001",
            open_date,
            None, None, None
        )
        cursor.execute(sql3, acc_data1)
        print("个人账户插入成功")

        # 企业账户
        acc_data2 = (
            "ABC001",
            banks[1][0],
            "企业1科技有限公司",
            "11",
            "62284804012345679001",
            None,
            "13",
            "23",
            "LICENSE20000000001",
            "U000001",
            open_date,
            None, None, None
        )
        cursor.execute(sql3, acc_data2)
        print("企业账户插入成功")

        # 第4步：插入风险等级测试
        print("\n第4步：插入风险等级测试...")
        sql4 = """INSERT INTO tb_risk_new (Bank_code1, Cst_no, Self_acc_name, Id_no, Acc_type,
                                        Risk_code, Time, Norm)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""

        # 个人风险
        risk_data1 = (
            banks[0][0],
            "P000001",
            "客户1",
            "1101011990010101234",
            "11",
            "02",
            "20241201",
            "客户1的风险评估记录"
        )
        cursor.execute(sql4, risk_data1)
        print("个人风险等级插入成功")

        # 企业风险
        risk_data2 = (
            banks[1][0],
            "U000001",
            "企业1科技有限公司",
            "LICENSE20000000001",
            "11",
            "03",
            "20241201",
            "企业1的风险评估记录"
        )
        cursor.execute(sql4, risk_data2)
        print("企业风险等级插入成功")

        # 第5步：插入交易记录测试
        print("\n第5步：插入交易记录测试...")
        sql5 = """INSERT INTO tb_acc_txn (Date, Time, Self_bank_code, Acc_type, Cst_no, Id_no, Self_acc_no,
                                        Card_no, Part_acc_no, Part_acc_name, Lend_flag, Tsf_flag, Reverse_flag,
                                        Cur, Org_amt, Usd_amt, Rmb_amt, Balance, Purpose, Bord_flag, Nation,
                                        Bank_flag, Ip_code, Atm_code, Bank_code, Mac_info, Settle_type, Ticd)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s)"""

        txn_data = (
            "20241201", "100000",
            banks[0][0],
            "11",
            "P000001",
            "1101011990010101234",
            "62284804012345678901",
            "62254804012345678901",
            "622848040987654321001",
            "交易对手1",
            "10",
            "11",
            "10",
            "CNY",
            5000.00,
            650.00,
            5000.00,
            10000.00,
            "交易记录1",
            "11",
            "CHN",
            "11",
            "192.168.1.1",
            "ATM001",
            banks[0][0],
            "IMEI123456789",
            "ST001",
            "TXN20241201001"
        )
        cursor.execute(sql5, txn_data)
        print("交易记录插入成功")

        # 第6步：插入报告测试
        print("\n第6步：插入报告测试...")
        sql6 = """INSERT INTO tb_lar_report (RLFC, ROTF, RPMN, RPMT, Report_Date,
                                          Institution_Name, Report_Amount, Currency,
                                          Transaction_Type, Transaction_Date,
                                          Customer_Name, Customer_ID, Account_No)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        lar_report = (
            "00",
            None,
            "RPM0000000000000001",
            None,
            "20241201",
            "中国农业银行总行营业部",
            1000000.00,
            "CNY",
            "现金存款",
            "20241201",
            "客户1",
            "P000001",
            "62284804012345678901"
        )
        cursor.execute(sql6, lar_report)
        print("大额交易报告插入成功")

        # 提交测试数据
        conn.commit()
        print("\n测试数据提交成功")

        # 验证测试结果
        print("\n=== 测试数据验证 ===")
        test_tables = [
            'tb_cst_pers', 'tb_cst_unit', 'tb_acc', 'tb_risk_new',
            'tb_acc_txn', 'tb_lar_report'
        ]

        success_count = 0
        for table in test_tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                if count > 0:
                    print(f"  {table:15} : {count} 条记录 [OK]")
                    success_count += 1
                else:
                    print(f"  {table:15} : {count} 条记录 [问题]")
            except Exception as e:
                print(f"  {table:15} : 查询失败 - {e}")

        print(f"\n测试结果: {success_count}/{len(test_tables)} 张表有数据")

        if success_count >= len(test_tables) * 0.8:
            print("测试基本成功，可以批量扩展数据")
        else:
            print("测试失败，需要检查表结构")

        cursor.close()
        conn.close()
        return success_count >= len(test_tables) * 0.8

    except Exception as e:
        print(f"[ERROR] 插入失败: {e}")
        if 'conn' in locals():
            conn.rollback()
        return False

if __name__ == "__main__":
    success = step_by_step_data()
    sys.exit(0 if success else 1)