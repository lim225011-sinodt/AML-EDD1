#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
清理现有数据并重新生成测试数据
"""

import mysql.connector
import random
import sys

def clean_and_generate_test_data():
    """清理数据并生成测试数据"""
    print("=== 清理数据并生成测试数据 ===\n")

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

        # 清理所有测试表数据
        print("清理现有测试数据...")
        tables_to_clean = [
            'tb_lar_report', 'tb_sus_report', 'tb_lwhc_log', 'tb_cross_border',
            'tb_cash_convert', 'tb_cash_remit', 'tb_cred_txn', 'tb_acc_txn',
            'tb_risk_his', 'tb_risk_new', 'tb_acc', 'tb_cst_unit', 'tb_cst_pers'
        ]

        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        for table in tables_to_clean:
            cursor.execute(f"DELETE FROM {table}")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        print("数据清理完成")

        # 生成测试数据
        print("\n开始生成测试数据...")

        # 1. 个人客户 (10个)
        print("1. 创建个人客户...")
        for i in range(1, 11):
            sql = """INSERT INTO tb_cst_pers (Head_no, Bank_code1, Cst_no, Open_time, Close_time, Acc_name,
                                            Cst_sex, Nation, Id_type, Id_no, Id_deadline, Occupation, Income,
                                            Contact1, Contact2, Contact3, Address1, Address2, Address3, Company, Sys_name)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

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

        print(f"已创建 10 个个人客户")

        # 2. 企业客户 (2个)
        print("2. 创建企业客户...")
        for i in range(1, 3):
            sql2 = """INSERT INTO tb_cst_unit (Head_no, Bank_code1, Cst_no, Open_time, Acc_name, Rep_name,
                                            Ope_name, License, Id_deadline, Industry, Reg_amt, Reg_amt_code, Sys_name)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

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

        print(f"已创建 2 个企业客户")

        # 3. 账户 (为每个客户创建账户)
        print("3. 创建账户...")
        for i in range(1, 13):  # 10个个人+2个企业=12个账户
            sql3 = """INSERT INTO tb_acc (Head_no, Bank_code1, Self_acc_name, Acc_state, Self_acc_no, Card_no,
                                        Acc_type, Acc_type1, Id_no, Cst_no, Open_time, Close_time,
                                        Agency_flag, Acc_flag, Fixed_flag)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            if i <= 10:  # 个人账户
                cst_no = f"P{i:06d}"
                acc_name = f"测试客户{i}"
                id_no = f"1101011990{i:02d}0101234"
                acc_type = '11'
                acc_type1 = '21'
                card_no = f"622548040123456789{i}"
            else:  # 企业账户
                cst_no = f"U{i-10:06d}"
                acc_name = f"测试企业{i-10}科技有限公司"
                id_no = f"LICENSE{str(i-10).zfill(10)}"
                acc_type = '13'
                acc_type1 = '23'
                card_no = None

            test_account = (
                "ABC001",
                "103100000019",
                acc_name,
                '11',
                f"622848040123456789{i}",
                card_no,
                acc_type,
                acc_type1,
                id_no,
                cst_no,
                '20240101',
                None, None, None
            )
            cursor.execute(sql3, test_account)

        print(f"已创建 12 个账户")

        # 4. 最新风险等级
        print("4. 创建最新风险等级...")
        for i in range(1, 13):
            sql4 = """INSERT INTO tb_risk_new (Bank_code1, Cst_no, Self_acc_name, Id_no, Acc_type,
                                            Risk_code, Time, Norm)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""

            if i <= 10:
                cst_no = f"P{i:06d}"
                acc_name = f"测试客户{i}"
                id_no = f"1101011990{i:02d}0101234"
            else:
                cst_no = f"U{i-10:06d}"
                acc_name = f"测试企业{i-10}科技有限公司"
                id_no = f"LICENSE{str(i-10).zfill(10)}"

            test_risk = (
                "103100000019",
                cst_no,
                acc_name,
                id_no,
                '11',
                random.choice(['01', '02', '03', '04']),
                '20241201',
                f'风险等级评估记录{i}'
            )
            cursor.execute(sql4, test_risk)

        print(f"已创建 12 条最新风险等级记录")

        # 5. 历史风险等级
        print("5. 创建历史风险等级...")
        for i in range(1, 8):  # 前7个客户的历史记录
            sql5 = """INSERT INTO tb_risk_his (Bank_code1, Cst_no, Self_acc_name, Id_no, Acc_type,
                                            Risk_code, Time, Norm)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""

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
            cursor.execute(sql5, test_risk_his)

        print(f"已创建 7 条历史风险等级记录")

        # 6. 账户交易记录
        print("6. 创建账户交易记录...")
        for i in range(1, 21):  # 20条交易记录
            sql6 = """INSERT INTO tb_acc_txn (Date, Time, Self_bank_code, Acc_type, Cst_no, Id_no, Self_acc_no,
                                            Card_no, Part_acc_no, Part_acc_name, Lend_flag, Tsf_flag, Reverse_flag,
                                            Cur, Org_amt, Usd_amt, Rmb_amt, Balance, Purpose, Bord_flag, Nation,
                                            Bank_flag, Ip_code, Atm_code, Bank_code, Mac_info, Settle_type, Ticd)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s)"""

            date = f"202412{str(i).zfill(2)}" if i <= 9 else f"202412{i-9}"
            time = f"10{str(i%24).zfill(2)}00"
            cst_num = (i-1) % 12 + 1  # 循环使用12个账户

            if cst_num <= 10:
                cst_no = f"P{cst_num:06d}"
                id_no = f"1101011990{cst_num:02d}0101234"
                acc_no = f"622848040123456789{cst_num}"
                card_no = f"622548040123456789{cst_num}"
            else:
                cst_no = f"U{cst_num-10:06d}"
                id_no = f"LICENSE{str(cst_num-10).zfill(10)}"
                acc_no = f"622848040123456789{cst_num}"
                card_no = None

            test_acc_txn = (
                date[:8], time,  # 确保日期是8位
                "103100000019",
                '11',
                cst_no,
                id_no,
                acc_no,
                card_no,
                f"622848040987654321{i}",
                f"交易对手{i}",
                random.choice(['10', '11']),
                random.choice(['10', '11']),
                '10',
                random.choice(['CNY', 'USD']),
                5000.00 + i*1000,
                650.00 + i*130,
                5000.00 + i*1000,
                10000.00 + i*500,
                f'测试交易{i}',
                random.choice(['11', '12']),
                random.choice(['USA', 'GBR', 'HKG']),
                '11',
                '192.168.1.1',
                'ATM001',
                '103100000019',
                'IMEI123456789',
                'ST001',
                f"TXN{date}{str(i).zfill(3)}"
            )
            cursor.execute(sql6, test_acc_txn)

        print(f"已创建 20 条账户交易记录")

        # 7. 信用卡交易记录
        print("7. 创建信用卡交易记录...")
        for i in range(1, 11):  # 10条信用卡交易
            sql7 = """INSERT INTO tb_cred_txn (Self_acc_no, Card_no, Self_acc_name, Cst_no, Id_no,
                                            Date, Time, Lend_flag, Tsf_flag, Cur, Org_amt, Usd_amt, Rmb_amt,
                                            Balance, Purpose, Pos_owner, Trans_type, Ip_code, Bord_flag, Nation)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            date = f"202412{str(i).zfill(2)}" if i <= 9 else f"202412{i-9}"
            time = f"14{str(i%24).zfill(2)}00"

            test_cred_txn = (
                f"6225{random.randint(1000000000000000, 9999999999999999)}",
                f"622548040123456789{i}",
                f"测试客户{i}",
                f"P{i:06d}",
                f"1101011990{i:02d}0101234",
                date[:8],
                time,
                random.choice(['10', '11']),
                random.choice(['10', '11']),
                'CNY',
                2000.00 + i*500,
                260.00 + i*65,
                2000.00 + i*500,
                5000.00 + i*200,
                random.choice(['POS消费', '网银支付', '取现', '还款']),
                random.choice(['沃尔玛超市', '天猫商城', '京东购物', '餐饮消费']),
                random.choice(['11', '12', '13']),
                '192.168.1.1',
                '12',
                'USA'
            )
            cursor.execute(sql7, test_cred_txn)

        print(f"已创建 10 条信用卡交易记录")

        # 8. 跨境交易记录
        print("8. 创建跨境交易记录...")
        for i in range(1, 9):  # 8条跨境交易
            sql8 = """INSERT INTO tb_cross_border (Date, Time, Lend_flag, Tsf_flag, Unit_flag, Cst_no, Id_no,
                                                Self_acc_name, Self_acc_no, Card_no, Self_add, Ticd, Part_acc_no,
                                                Part_acc_name, Part_nation, Cur, Org_amt, Usd_amt, Rmb_amt,
                                                Balance, Agency_flag, Agent_name, Agent_tel, Agent_type, Agent_no,
                                                Settle_type, Reverse_flag, Purpose, Bord_flag, Nation, Bank_flag,
                                                Ip_code, Atm_code, Bank_code, Mac_info)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            date = f"202412{str(i).zfill(2)}" if i <= 9 else f"202412{i-9}"
            time = f"16{str(i%24).zfill(2)}00"
            cst_num = i if i <= 10 else i-10

            test_cross_border = (
                date[:8], time,
                random.choice(['10', '11']),
                random.choice(['10', '11']),
                random.choice(['11', '12']),
                f"P{cst_num:06d}",
                f"1101011990{cst_num:02d}0101234",
                f"测试客户{cst_num}",
                f"622848040123456789{cst_num}",
                f"622548040123456789{cst_num}",
                f"北京市朝阳区跨境业务部{i}",
                f"CB{date}{str(i).zfill(3)}",
                f"FOREIGN{str(i).zfill(10)}",
                f"海外公司{i}",
                random.choice(['USA', 'GBR', 'JPN', 'HKG']),
                'USD',
                10000.00 + i*2000,
                10000.00 + i*2000,
                68000.00 + i*13600,
                20000.00 + i*4000,
                '11', None, None, None, None,
                'ST001',
                '10',
                '货物贸易',
                '11',
                random.choice(['USA', 'GBR', 'JPN']),
                '11',
                '192.168.1.1',
                'ATM001',
                '103100000019',
                'IMEI123456789'
            )
            cursor.execute(sql8, test_cross_border)

        print(f"已创建 8 条跨境交易记录")

        # 9. 现金交易记录
        print("9. 创建现金交易记录...")

        # 现金汇款
        for i in range(1, 8):  # 6条现金汇款
            sql9 = """INSERT INTO tb_cash_remit (Date, Time, Self_bank_code, Acc_name, Id_no,
                                              Cur, Org_amt, Usd_amt, Rmb_amt, Part_bank,
                                              Part_acc_no, Part_acc_name, Settle_type, Ticd)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            date = f"202412{str(i).zfill(2)}" if i <= 9 else f"202412{i-9}"
            time = f"11{str(i%24).zfill(2)}00"

            test_cash_remit = (
                date[:8], time,
                '103100000019',
                f"测试客户{i}",
                f"1101011990{i:02d}0101234",
                'CNY',
                8000.00 + i*1500,
                1040.00 + i*195,
                8000.00 + i*1500,
                '工商银行',
                f"622848040987654321{i}",
                f"现金收款人{i}",
                'ST001',
                f"CR{date}{str(i).zfill(3)}"
            )
            cursor.execute(sql9, test_cash_remit)

        print(f"已创建 6 条现金汇款记录")

        # 现钞结售汇
        for i in range(1, 5):  # 4条现钞结售汇
            sql10 = """INSERT INTO tb_cash_convert (Date, Time, Self_bank_code, Acc_name, Id_no,
                                                Out_cur, Out_org_amt, Out_usd_amt,
                                                In_cur, In_org_amt, In_usd_amt,
                                                Ticd, Counter_no, Settle_type)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            date = f"202412{str(i).zfill(2)}" if i <= 9 else f"202412{i-9}"
            time = f"13{str(i%24).zfill(2)}00"

            test_cash_convert = (
                date[:8], time,
                '103100000019',
                f"测试客户{i}",
                f"1101011990{i:02d}0101234",
                'USD',
                2000.00 + i*500,
                2000.00 + i*500,
                'CNY',
                13600.00 + i*3400,
                1768.00 + i*442,
                f"CC{date}{str(i).zfill(3)}",
                f"CT{str(i).zfill(4)}",
                'ST001'
            )
            cursor.execute(sql10, test_cash_convert)

        print(f"已创建 4 条现钞结售汇记录")

        # 10. 联网核查日志
        print("10. 创建联网核查日志...")
        for i in range(1, 15):  # 14条核查记录
            sql11 = """INSERT INTO tb_lwhc_log (Bank_name, Bank_code2, Date, Time, Name, Id_no,
                                             Result, Counter_no, Ope_line, Mode, Purpose)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            date = f"202412{str(i).zfill(2)}" if i <= 9 else f"202412{i-9}"
            time = f"15{str(i%24).zfill(2)}00"

            test_lwhc = (
                '中国农业银行总行营业部',
                '104100000004',
                date[:8],
                time,
                f"测试客户{i}",
                f"1101011990{i:02d}0101234",
                random.choice(['11', '12', '13']),
                f"CT{str(i).zfill(4)}",
                '个人金融',
                '10',
                '开户核查'
            )
            cursor.execute(sql11, test_lwhc)

        print(f"已创建 14 条联网核查日志记录")

        # 11. 大额交易报告
        print("11. 创建大额交易报告...")
        for i in range(1, 6):  # 5条大额报告
            sql12 = """INSERT INTO tb_lar_report (RLFC, ROTF, RPMN, RPMT, Report_Date,
                                              Institution_Name, Report_Amount, Currency,
                                              Transaction_Type, Transaction_Date,
                                              Customer_Name, Customer_ID, Account_No)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            test_lar = (
                random.choice(['00', '01', '02']),
                None,
                f"RPM{str(i).zfill(16)}",
                None,
                f"202412{str(i).zfill(2)}" if i <= 9 else f"202412{i-9}",
                '中国农业银行总行营业部',
                (1000000.00 * i) + random.randint(0, 100000),
                'CNY',
                random.choice(['现金存款', '现金取款', '转账', '跨境汇款']),
                f"202412{str(i).zfill(2)}" if i <= 9 else f"202412{i-9}",
                f"测试客户{i}",
                f"P{i:06d}",
                f"622848040123456789{i}"
            )
            cursor.execute(sql12, test_lar)

        print(f"已创建 5 条大额交易报告记录")

        # 12. 可疑交易报告
        print("12. 创建可疑交易报告...")
        for i in range(1, 4):  # 3条可疑报告
            sql13 = """INSERT INTO tb_sus_report (TBID, TBIT, TBNM, TBNT, TCAC, TCAT, TCID, TCIT, TCNM, TICD, TRCD,
                                              Report_Date, Institution_Name, Transaction_Amount, Currency,
                                              Transaction_Type, Suspicious_Reason, Report_Time)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            test_sus = (
                None, None, None, None, None, None, None, None, None,
                f"SUS202412{str(i).zfill(2)}{str(i).zfill(3)}",
                'CHN000000',
                f"202412{str(i).zfill(2)}" if i <= 9 else f"202412{i-9}",
                '中国农业银行总行营业部',
                500000.00 + i*100000,
                'CNY',
                random.choice(['洗钱风险', '恐怖融资', '欺诈交易']),
                random.choice(['交易金额与客户身份不符', '频繁的大额现金交易', '跨境交易异常']),
                f"16{str(i%24).zfill(2)}00"
            )
            cursor.execute(sql13, test_sus)

        print(f"已创建 3 条可疑交易报告记录")

        # 提交事务
        conn.commit()
        print(f"\n[SUCCESS] 完整的15张表测试数据生成成功！")

        # 验证所有表的数据
        print(f"\n=== 数据统计验证 ===")
        all_tables = [
            'tb_cst_pers', 'tb_cst_unit', 'tb_acc', 'tb_risk_new', 'tb_risk_his',
            'tb_acc_txn', 'tb_cred_txn', 'tb_cross_border', 'tb_cash_remit', 'tb_cash_convert',
            'tb_lwhc_log', 'tb_lar_report', 'tb_sus_report', 'tb_bank', 'tb_settle_type'
        ]

        total_records = 0
        for table in all_tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                total_records += count

                if count > 0:
                    print(f"  ✅ {table:15} : {count:8d} 条记录")
                else:
                    print(f"  ⚠️  {table:15} : {count:8d} 条记录")
            except Exception as e:
                print(f"  ❌ {table:15} : 查询失败 - {e}")

        print(f"\n📊 总计: {total_records:,} 条记录")
        print(f"\n✅ 覆盖情况:")
        print(f"  - 个人客户: 10 个")
        print(f"  - 企业客户: 2 个")
        print(f"  - 账户: 12 个")
        print(f"  - 各类交易: 48 条")
        print(f"  - 风险记录: 19 条")
        print(f"  - 报告记录: 8 条")
        print(f"  - 核查日志: 14 条")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"[ERROR] 生成测试数据失败: {e}")
        if 'conn' in locals():
            conn.rollback()
        return False

if __name__ == "__main__":
    success = clean_and_generate_test_data()
    sys.exit(0 if success else 1)