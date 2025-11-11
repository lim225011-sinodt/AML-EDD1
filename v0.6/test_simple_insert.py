#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简单测试：逐条插入数据来调试问题
"""

import mysql.connector
import random
import sys

def test_simple_insert():
    """测试简单插入"""
    print("=== 测试简单数据插入 ===\n")

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

        # 测试插入一条个人客户记录
        print("测试插入个人客户...")
        test_data = (
            "ABC001",  # Head_no
            "103100000019",  # Bank_code1 (使用存在的银行代码)
            "P000001",  # Cst_no
            "20240101",  # Open_time
            None,  # Close_time
            "张三",  # Acc_name
            "11",  # Cst_sex
            "CHN",  # Nation
            "11",  # Id_type
            "110101199001011234",  # Id_no
            "20300101",  # Id_deadline
            "工程师",  # Occupation
            100000.00,  # Income
            "13800138000",  # Contact1
            None, None,  # Contact2, Contact3
            "北京市朝阳区1号",  # Address1
            None, None,  # Address2, Address3
            "某科技公司",  # Company
            "系统001"  # Sys_name
        )

        sql = """INSERT INTO tb_cst_pers (Head_no, Bank_code1, Cst_no, Open_time, Close_time, Acc_name,
                                         Cst_sex, Nation, Id_type, Id_no, Id_deadline, Occupation, Income,
                                         Contact1, Contact2, Contact3, Address1, Address2, Address3, Company, Sys_name)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        cursor.execute(sql, test_data)
        print("插入一条个人客户记录成功")

        # 测试插入一条企业客户记录
        print("测试插入企业客户...")
        test_company = (
            "ABC001",  # Head_no
            "103100000027",  # Bank_code1 (使用存在的银行代码)
            "U000001",  # Cst_no
            "20240101",  # Open_time
            "华兴科技有限公司",  # Acc_name
            "法人张三",  # Rep_name
            "经办人李四",  # Ope_name
            "LICENSE123456789",  # License
            "20300101",  # Id_deadline
            "科技",  # Industry
            10000000.00,  # Reg_amt
            "CNY",  # Reg_amt_code
            "企业系统001"  # Sys_name
        )

        sql2 = """INSERT INTO tb_cst_unit (Head_no, Bank_code1, Cst_no, Open_time, Acc_name, Rep_name,
                                          Ope_name, License, Id_deadline, Industry, Reg_amt, Reg_amt_code, Sys_name)
                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        cursor.execute(sql2, test_company)
        print("插入一条企业客户记录成功")

        # 测试插入一条账户记录
        print("测试插入账户...")
        test_account = (
            "H001",  # Head_no
            "103100000019",  # Bank_code1
            "张三",  # Self_acc_name
            "11",  # Acc_state
            "6228480401234567890",  # Self_acc_no
            "6225480401234567890",  # Card_no
            "11",  # Acc_type
            "21",  # Acc_type1
            "110101199001011234",  # Id_no
            "P000001",  # Cst_no
            "20240101",  # Open_time
            None,  # Close_time
            None,  # Agency_flag
            None,  # Acc_flag
            None   # Fixed_flag
        )

        sql3 = """INSERT INTO tb_acc (Head_no, Bank_code1, Self_acc_name, Acc_state, Self_acc_no, Card_no,
                                      Acc_type, Acc_type1, Id_no, Cst_no, Open_time, Close_time,
                                      Agency_flag, Acc_flag, Fixed_flag)
                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        cursor.execute(sql3, test_account)
        print("插入一条账户记录成功")

        # 测试插入风险等级记录
        print("测试插入风险等级...")
        test_risk = (
            "103100000019",  # Bank_code1
            "P000001",  # Cst_no
            "张三",  # Self_acc_name
            "110101199001011234",  # Id_no
            "11",  # Acc_type
            "02",  # Risk_code (中风险)
            "20241201",  # Time
            "客户风险等级为中等，需要定期关注"  # Norm
        )

        sql4 = """INSERT INTO tb_risk_new (Bank_code1, Cst_no, Self_acc_name, Id_no, Acc_type,
                                          Risk_code, Time, Norm)
                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""

        cursor.execute(sql4, test_risk)
        print("插入一条风险等级记录成功")

        # 提交事务
        conn.commit()
        print("\n[SUCCESS] 所有测试数据插入成功！")

        # 扩展插入更多数据来覆盖所有表
        print("\n开始扩展插入覆盖所有15张表的数据...")

        # 2. 更多个人客户 (共5个)
        for i in range(2, 6):
            test_data = (
                "ABC001",
                random.choice(["103100000019", "103100000027", "103100000035"]),
                f"P{i:06d}",
                "20240101",
                None,
                f"测试客户{i}",
                random.choice(['11', '12']),
                'CHN',
                '11',
                f"1101011990{i:02d}0101234",
                '20300101',
                '软件工程师',
                150000.00 + i*10000,
                f"139{str(i).zfill(8)}",
                None, None,
                f"北京市测试地址{i}号",
                None, None,
                f"测试公司{i}",
                f"测试系统{i}"
            )
            cursor.execute(sql, test_data)

        # 3. 更多企业客户 (共2个)
        for i in range(2, 3):
            test_company = (
                "ABC001",
                "103100000035",
                f"U{i:06d}",
                "20240101",
                f"测试企业{i}科技有限公司",
                f"测试法人{i}",
                f"测试经办{i}",
                f"LICENSE{str(i).zfill(10)}",
                '20300101',
                '软件开发',
                20000000.00,
                'CNY',
                f"企业系统{i}"
            )
            cursor.execute(sql2, test_company)

        # 4. 更多账户 (为每个客户创建账户)
        for i in range(2, 7):
            test_account = (
                "ABC001",
                "103100000019",
                f"测试客户{i}",
                '11',
                f"622848040123456789{i}",
                f"622548040123456789{i}",
                '11',
                '21',
                f"1101011990{i:02d}0101234",
                f"P{i:06d}",
                '20240101',
                None, None, None
            )
            cursor.execute(sql3, test_account)

        # 5. 历史风险等级
        for i in range(1, 4):
            test_risk_his = (
                "103100000019",
                f"P{i:06d}",
                f"测试客户{i}",
                f"1101011990{i:02d}0101234",
                '11',
                random.choice(['01', '02', '03']),
                '20231201',
                f"历史风险等级记录{i}"
            )
            sql5 = """INSERT INTO tb_risk_his (Bank_code1, Cst_no, Self_acc_name, Id_no, Acc_type,
                                              Risk_code, Time, Norm)
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
            cursor.execute(sql5, test_risk_his)

        # 6. 账户交易记录 (简单几条测试)
        for i in range(1, 4):
            date = f"2024120{i}"
            time = f"10{str(i).zfill(2)}00"
            test_acc_txn = (
                date, time,
                "103100000019",  # Self_bank_code
                '11',  # Acc_type
                f"P{i:06d}",  # Cst_no
                f"1101011990{i:02d}0101234",  # Id_no
                f"622848040123456789{i}",  # Self_acc_no
                f"622548040123456789{i}",  # Card_no
                f"622848040987654321{i}",  # Part_acc_no
                f"交易对手{i}",
                '10',  # Lend_flag
                '11',  # Tsf_flag
                '10',  # Reverse_flag
                'CNY',  # Cur
                5000.00 + i*1000,  # Org_amt
                650.00 + i*130,  # Usd_amt
                5000.00 + i*1000,  # Rmb_amt
                10000.00,  # Balance
                f'测试交易{i}',
                '12',  # Bord_flag
                'USA',  # Nation
                '11',  # Bank_flag
                '192.168.1.1',  # Ip_code
                'ATM001',  # Atm_code
                '103100000019',  # Bank_code
                'IMEI123456789',  # Mac_info
                'ST001',  # Settle_type
                f"TXN{date}{str(i).zfill(3)}"  # Ticd
            )
            sql6 = """INSERT INTO tb_acc_txn (Date, Time, Self_bank_code, Acc_type, Cst_no, Id_no, Self_acc_no,
                                            Card_no, Part_acc_no, Part_acc_name, Lend_flag, Tsf_flag, Reverse_flag,
                                            Cur, Org_amt, Usd_amt, Rmb_amt, Balance, Purpose, Bord_flag, Nation,
                                            Bank_flag, Ip_code, Atm_code, Bank_code, Mac_info, Settle_type, Ticd)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s)"""
            cursor.execute(sql6, test_acc_txn)

        # 7. 联网核查日志
        for i in range(1, 4):
            test_lwhc = (
                '中国农业银行总行营业部',  # Bank_name
                '104100000004',  # Bank_code2
                f"2024120{i}",  # Date
                f"14{str(i).zfill(2)}00",  # Time
                f"测试客户{i}",
                f"1101011990{i:02d}0101234",
                random.choice(['11', '12', '13']),  # Result
                f"CT00{i}",
                '个人金融',
                '10',  # Mode
                '开户核查'
            )
            sql7 = """INSERT INTO tb_lwhc_log (Bank_name, Bank_code2, Date, Time, Name, Id_no,
                                             Result, Counter_no, Ope_line, Mode, Purpose)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            cursor.execute(sql7, test_lwhc)

        # 8. 大额交易报告
        for i in range(1, 3):
            test_lar = (
                '00',  # RLFC
                None,  # ROTF
                f"RPM{str(i).zfill(16)}",  # RPMN
                None,  # RPMT
                f"2024120{i}",
                '中国农业银行总行营业部',
                1000000.00 * i,  # Report_Amount
                'CNY',
                '现金存款',
                f"2024120{i}",
                f"测试客户{i}",
                f"P{i:06d}",
                f"622848040123456789{i}"
            )
            sql8 = """INSERT INTO tb_lar_report (RLFC, ROTF, RPMN, RPMT, Report_Date,
                                              Institution_Name, Report_Amount, Currency,
                                              Transaction_Type, Transaction_Date,
                                              Customer_Name, Customer_ID, Account_No)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            cursor.execute(sql8, test_lar)

        # 9. 可疑交易报告
        for i in range(1, 2):
            test_sus = (
                None, None, None, None, None, None, None, None, None,  # TBID, TBIT, TBNM, TBNT, TCAC, TCAT, TCID, TCIT, TCNM
                f"SUS2024120{i}{str(i).zfill(3)}",  # TICD
                'CHN000000',  # TRCD
                f"2024120{i}",
                '中国农业银行总行营业部',
                500000.00,  # Transaction_Amount
                'CNY',
                '洗钱风险',
                '交易金额与客户身份不符',
                f"14{str(i).zfill(2)}00"  # Report_Time
            )
            sql9 = """INSERT INTO tb_sus_report (TBID, TBIT, TBNM, TBNT, TCAC, TCAT, TCID, TCIT, TCNM, TICD, TRCD,
                                              Report_Date, Institution_Name, Transaction_Amount, Currency,
                                              Transaction_Type, Suspicious_Reason, Report_Time)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            cursor.execute(sql9, test_sus)

        print("扩展数据插入完成")

        # 验证插入的数据
        print("\n验证插入的数据:")
        tables_to_check = ['tb_cst_pers', 'tb_cst_unit', 'tb_acc', 'tb_risk_new', 'tb_risk_his',
                          'tb_acc_txn', 'tb_lwhc_log', 'tb_lar_report', 'tb_sus_report']
        for table in tables_to_check:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"  {table:15} : {count} 条记录")
            except Exception as e:
                print(f"  {table:15} : 查询失败 - {e}")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"[ERROR] 插入测试数据失败: {e}")
        if 'conn' in locals():
            conn.rollback()
        return False

if __name__ == "__main__":
    success = test_simple_insert()
    sys.exit(0 if success else 1)