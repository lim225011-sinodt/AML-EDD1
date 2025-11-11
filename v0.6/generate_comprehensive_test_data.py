#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
生成完整的测试数据覆盖所有15张表
10个个人客户 + 2个企业客户 + 完整的交易/风险/报告数据
"""

import mysql.connector
import random
import string
from datetime import datetime, timedelta
import sys

class ComprehensiveTestDataGenerator:
    """完整测试数据生成器"""

    def __init__(self):
        self.conn = mysql.connector.connect(
            host='101.42.102.9',
            port=3306,
            user='root',
            password='Bancstone123!',
            database='AML300',
            charset='utf8mb4'
        )
        self.cursor = self.conn.cursor()

        # 中文姓氏和名字
        self.surnames = ['张', '王', '李', '赵', '刘', '陈', '杨', '黄', '周', '吴']
        self.given_names = ['伟', '芳', '娜', '敏', '静', '丽', '强', '磊', '军', '洋']

        # 企业类型
        self.company_types = ['科技有限公司', '贸易有限公司', '投资公司']
        self.company_suffixes = ['有限公司', '股份有限公司']

        # 银行代码（从现有数据中获取）
        self.bank_codes = ['103100000019', '103100000027', '103100000035', '103100000043', '103100000050']
        self.settle_types = ['ST001', 'ST002', 'ST003', 'ST004', 'ST005', 'ST006', 'ST007', 'ST008', 'ST009', 'ST010']

        print("成功连接到AML300数据库，准备生成完整测试数据")

    def generate_id_number(self):
        """生成身份证号码"""
        area_code = random.choice(['110101', '310101', '440103', '330102', '510104'])
        birth_date = f"{random.randint(1980, 2000):04d}{random.randint(1, 12):02d}{random.randint(1, 28):02d}"
        sequence = f"{random.randint(1, 999):03d}"
        check_code = random.choice('0123456789X')
        return area_code + birth_date + sequence + check_code

    def generate_random_date(self, start_date='2023-01-01', end_date='2025-01-09'):
        """生成随机日期"""
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        delta = end - start
        random_days = random.randint(0, delta.days)
        return (start + timedelta(days=random_days)).strftime('%Y-%m-%d')

    def generate_random_date_char8(self, start_date='2023-01-01', end_date='2025-01-09'):
        """生成CHAR(8)格式的日期"""
        return self.generate_random_date(start_date, end_date).replace('-', '')

    def generate_random_time_char6(self):
        """生成CHAR(6)格式的时间"""
        return f"{random.randint(8, 20):02d}{random.randint(0, 59):02d}{random.randint(0, 59):02d}"

    def generate_random_amount(self, min_val=100, max_val=1000000):
        """生成随机金额"""
        return round(random.uniform(min_val, max_val), 2)

    def clear_existing_data(self):
        """清理现有测试数据"""
        print("清理现有测试数据...")
        tables = [
            'tb_lar_report', 'tb_sus_report', 'tb_lwhc_log', 'tb_cross_border',
            'tb_cash_convert', 'tb_cash_remit', 'tb_cred_txn', 'tb_acc_txn',
            'tb_risk_his', 'tb_risk_new', 'tb_acc', 'tb_cst_unit', 'tb_cst_pers'
        ]

        self.cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        for table in tables:
            self.cursor.execute(f"DELETE FROM {table}")
        self.cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        print("数据清理完成")

    def create_individual_customers(self, count=1):
        """创建个人客户"""
        print(f"创建 {count} 个个人客户...")
        cst_pers_data = []

        for i in range(1, count + 1):
            surname = random.choice(self.surnames)
            given_name = random.choice(self.given_names)
            name = surname + given_name
            id_no = self.generate_id_number()

            cst_pers_data.append((
                "ABC001",  # Head_no
                random.choice(self.bank_codes),  # Bank_code1
                f"P{i:06d}",  # Cst_no
                self.generate_random_date_char8('2020-01-01', '2024-12-31'),  # Open_time
                None,  # Close_time
                name,  # Acc_name
                random.choice(['11', '12']),  # Cst_sex
                'CHN',  # Nation
                '11',  # Id_type (身份证)
                id_no,  # Id_no
                '20300101',  # Id_deadline
                random.choice(['工程师', '教师', '医生', '销售', '管理', '金融分析师']),  # Occupation
                self.generate_random_amount(50000, 500000),  # Income
                f"138{random.randint(10000000, 99999999)}",  # Contact1
                None, None,  # Contact2, Contact3
                f"北京市朝阳区{i}号院{i}号楼",  # Address1
                None, None,  # Address2, Address3
                f"某科技集团{i}分公司",  # Company
                f"个人客户系统{i:03d}"  # Sys_name
            ))

        self.cursor.executemany("""
            INSERT INTO tb_cst_pers (Head_no, Bank_code1, Cst_no, Open_time, Close_time, Acc_name,
                                    Cst_sex, Nation, Id_type, Id_no, Id_deadline, Occupation, Income,
                                    Contact1, Contact2, Contact3, Address1, Address2, Address3, Company, Sys_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, cst_pers_data[:1])  # 先测试1条
        print(f"已创建 {len(cst_pers_data[:1])} 条个人客户记录")
        return cst_pers_data

    def create_corporate_customers(self, count=1):
        """创建企业客户"""
        print(f"创建 {count} 个企业客户...")
        cst_unit_data = []

        for i in range(1, count + 1):
            company_type = random.choice(self.company_types)
            suffix = random.choice(self.company_suffixes)
            name = f"{random.choice(['华', '中', '国', '民', '金', '信'])}{random.choice(['兴', '发', '展', '达', '盛', '通'])}{company_type}{suffix}"

            cst_unit_data.append((
                "ABC001",  # Head_no
                random.choice(self.bank_codes),  # Bank_code1
                f"U{i:06d}",  # Cst_no
                self.generate_random_date_char8('2020-01-01', '2024-12-31'),  # Open_time
                name,  # Acc_name
                f"法人代表{random.randint(1, 999)}",  # Rep_name
                f"经办人{random.randint(1, 999)}",  # Ope_name
                f"LICENSE{random.randint(10000000, 99999999)}",  # License
                '20300101',  # Id_deadline
                random.choice(['软件开发', '国际贸易', '金融服务', '制造业', '咨询服务']),  # Industry
                self.generate_random_amount(1000000, 50000000),  # Reg_amt
                'CNY',  # Reg_amt_code
                f"企业客户系统{i:03d}"  # Sys_name
            ))

        self.cursor.executemany("""
            INSERT INTO tb_cst_unit (Head_no, Bank_code1, Cst_no, Open_time, Acc_name, Rep_name,
                                    Ope_name, License, Id_deadline, Industry, Reg_amt, Reg_amt_code, Sys_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, cst_unit_data)
        print(f"已创建 {len(cst_unit_data[:1])} 条企业客户记录")
        return cst_unit_data

    def create_accounts(self, pers_customers, unit_customers):
        """创建账户信息"""
        print("创建账户信息...")
        acc_data = []

        # 为个人客户创建账户
        for i, (cst_data) in enumerate(pers_customers, 1):
            cst_no = cst_data[2]  # Cst_no
            acc_name = cst_data[5]  # Acc_name

            acc_data.append((
                "ABC001",  # Head_no
                cst_data[1],  # Bank_code1
                acc_name,  # Self_acc_name
                random.choice(['11', '12']),  # Acc_state
                f"6228{random.randint(1000000000000000, 9999999999999999)}",  # Self_acc_no
                f"6225{random.randint(1000000000000000, 9999999999999999)}",  # Card_no
                random.choice(['11', '12']),  # Acc_type
                random.choice(['21', '22']),  # Acc_type1
                cst_data[9],  # Id_no
                cst_no,  # Cst_no
                self.generate_random_date_char8('2020-01-01', '2024-12-31'),  # Open_time
                None, None, None  # Close_time, Agency_flag, Acc_flag, Fixed_flag
            ))

        # 为企业客户创建账户
        for i, (cst_data) in enumerate(unit_customers, 1):
            cst_no = cst_data[2]  # Cst_no
            acc_name = cst_data[4]  # Acc_name

            acc_data.append((
                "ABC001",  # Head_no
                cst_data[1],  # Bank_code1
                acc_name,  # Self_acc_name
                '11',  # Acc_state (正常)
                f"6229{random.randint(1000000000000000, 9999999999999999)}",  # Self_acc_no (企业账户)
                None,  # Card_no (企业账户可能没有卡)
                '13',  # Acc_type (企业账户)
                '23',  # Acc_type1
                cst_data[6],  # License
                cst_no,  # Cst_no
                self.generate_random_date_char8('2020-01-01', '2024-12-31'),  # Open_time
                None, None, None  # Close_time, Agency_flag, Acc_flag, Fixed_flag
            ))

        self.cursor.executemany("""
            INSERT INTO tb_acc (Head_no, Bank_code1, Self_acc_name, Acc_state, Self_acc_no, Card_no,
                               Acc_type, Acc_type1, Id_no, Cst_no, Open_time, Close_time,
                               Agency_flag, Acc_flag, Fixed_flag)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, acc_data)
        print(f"已创建 {len(acc_data)} 条账户记录")
        return acc_data

    def create_risk_levels(self, pers_customers, unit_customers):
        """创建风险等级信息"""
        print("创建风险等级信息...")

        # 最新风险等级
        risk_new_data = []
        all_customers = pers_customers + unit_customers

        for cst_data in all_customers:
            bank_code = cst_data[1]  # Bank_code1
            cst_no = cst_data[2]  # Cst_no
            acc_name = cst_data[5] if len(cst_data) > 5 else cst_data[4]  # Acc_name
            id_no = cst_data[9] if len(cst_data) > 9 else cst_data[6]  # Id_no 或 License

            risk_code = random.choice(['01', '02', '03', '04'])  # 风险等级代码

            risk_new_data.append((
                bank_code, cst_no, acc_name, id_no,
                random.choice(['11', '12']),  # Acc_type
                risk_code,  # Risk_code
                self.generate_random_date_char8('2024-01-01', '2024-12-31'),  # Time
                f"风险等级{risk_code}的评估说明"  # Norm
            ))

        self.cursor.executemany("""
            INSERT INTO tb_risk_new (Bank_code1, Cst_no, Self_acc_name, Id_no, Acc_type,
                                   Risk_code, Time, Norm)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, risk_new_data)
        print(f"已创建 {len(risk_new_data)} 条最新风险等级记录")

        # 历史风险等级（为部分客户创建历史记录）
        risk_his_data = []
        for i, cst_data in enumerate(all_customers[:8]):  # 前8个客户创建历史记录
            bank_code = cst_data[1]  # Bank_code1
            cst_no = cst_data[2]  # Cst_no
            acc_name = cst_data[5] if len(cst_data) > 5 else cst_data[4]  # Acc_name
            id_no = cst_data[9] if len(cst_data) > 9 else cst_data[6]  # Id_no 或 License

            # 每个客户2-3条历史记录
            history_count = random.randint(2, 3)
            for j in range(history_count):
                risk_code = random.choice(['01', '02', '03', '04'])

                risk_his_data.append((
                    bank_code, cst_no, acc_name, id_no,
                    random.choice(['11', '12']),  # Acc_type
                    risk_code,  # Risk_code
                    self.generate_random_date_char8('2022-01-01', '2023-12-31'),  # Time
                    f"历史风险等级{risk_code}的记录-第{j+1}次"  # Norm
                ))

        self.cursor.executemany("""
            INSERT INTO tb_risk_his (Bank_code1, Cst_no, Self_acc_name, Id_no, Acc_type,
                                   Risk_code, Time, Norm)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, risk_his_data)
        print(f"已创建 {len(risk_his_data)} 条历史风险等级记录")

    def create_account_transactions(self, accounts):
        """创建账户交易记录"""
        print("创建账户交易记录...")
        acc_txn_data = []

        for i, acc_data in enumerate(accounts):
            bank_code = acc_data[1]  # Bank_code1
            acc_no = acc_data[4]  # Self_acc_no
            acc_name = acc_data[2]  # Self_acc_name
            cst_no = acc_data[9]  # Cst_no
            id_no = acc_data[8]  # Id_no

            # 每个账户创建5-15条交易记录
            txn_count = random.randint(5, 15)
            for j in range(txn_count):
                date = self.generate_random_date_char8('2024-01-01', '2024-12-31')
                time = self.generate_random_time_char6()

                org_amt = self.generate_random_amount(100, 500000)
                usd_amt = round(org_amt * random.uniform(0.13, 0.15), 2)
                rmb_amt = org_amt

                acc_txn_data.append((
                    date, time,
                    bank_code,  # Self_bank_code
                    random.choice(['11', '12']),  # Acc_type
                    cst_no, id_no, acc_no,
                    acc_data[5],  # Card_no
                    f"6228{random.randint(1000000000000000, 9999999999999999)}",  # Part_acc_no
                    f"交易对手{j+1}",  # Part_acc_name
                    random.choice(['10', '11']),  # Lend_flag (收付)
                    random.choice(['10', '11']),  # Tsf_flag (现金/转账)
                    random.choice(['10', '11']),  # Reverse_flag
                    random.choice(['CNY', 'USD']),  # Cur
                    org_amt, usd_amt, rmb_amt,
                    self.generate_random_amount(0, 100000),  # Balance
                    random.choice(['日常消费', '转账汇款', '工资收入', '还款', '充值', '投资理财']),
                    random.choice(['11', '12']),  # Bord_flag
                    random.choice(['USA', 'GBR', 'HKG', 'SGP']),  # Nation
                    random.choice(['11', '12', '13']),  # Bank_flag
                    f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 255)}",  # Ip_code
                    f"ATM{random.randint(1000, 9999)}",  # Atm_code
                    bank_code,  # Bank_code
                    f"IMEI{random.randint(1000000000000000, 9999999999999999)}",  # Mac_info
                    random.choice(self.settle_types),  # Settle_type
                    f"TXN{date}{random.randint(1000, 9999)}"  # Ticd
                ))

        self.cursor.executemany("""
            INSERT INTO tb_acc_txn (Date, Time, Self_bank_code, Acc_type, Cst_no, Id_no, Self_acc_no,
                                   Card_no, Part_acc_no, Part_acc_name, Lend_flag, Tsf_flag, Reverse_flag,
                                   Cur, Org_amt, Usd_amt, Rmb_amt, Balance, Purpose, Bord_flag, Nation,
                                   Bank_flag, Ip_code, Atm_code, Bank_code, Mac_info, Settle_type, Ticd)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, acc_txn_data)
        print(f"已创建 {len(acc_txn_data)} 条账户交易记录")

    def create_credit_transactions(self, pers_customers):
        """创建信用卡交易记录"""
        print("创建信用卡交易记录...")
        cred_txn_data = []

        # 选择有卡号的个人客户
        customers_with_cards = pers_customers[:8]  # 前8个个人客户

        for i, cst_data in enumerate(customers_with_cards):
            cst_no = cst_data[2]  # Cst_no
            acc_name = cst_data[5]  # Acc_name
            id_no = cst_data[9]  # Id_no

            # 为每个客户创建3-8条信用卡交易
            txn_count = random.randint(3, 8)
            for j in range(txn_count):
                date = self.generate_random_date_char8('2024-01-01', '2024-12-31')
                time = self.generate_random_time_char6()

                org_amt = self.generate_random_amount(100, 100000)
                usd_amt = round(org_amt * random.uniform(0.13, 0.15), 2)
                rmb_amt = org_amt

                cred_txn_data.append((
                    f"6225{random.randint(1000000000000000, 9999999999999999)}",  # Self_acc_no
                    f"6225{random.randint(1000000000000000, 9999999999999999)}",  # Card_no
                    acc_name, cst_no, id_no,
                    date, time,
                    random.choice(['10', '11']),  # Lend_flag
                    random.choice(['10', '11']),  # Tsf_flag
                    random.choice(['CNY', 'USD']),  # Cur
                    org_amt, usd_amt, rmb_amt,
                    self.generate_random_amount(-10000, 5000),  # Balance
                    random.choice(['POS消费', '网银支付', '取现', '还款', '转账']),
                    random.choice(['沃尔玛超市', '天猫商城', '京东购物', '餐饮消费', '加油卡充值']),
                    random.choice(['11', '12', '13']),  # Trans_type
                    f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 255)}",  # Ip_code
                    random.choice(['11', '12']),  # Bord_flag
                    random.choice(['USA', 'GBR', 'JPN'])  # Nation
                ))

        self.cursor.executemany("""
            INSERT INTO tb_cred_txn (Self_acc_no, Card_no, Self_acc_name, Cst_no, Id_no,
                                    Date, Time, Lend_flag, Tsf_flag, Cur, Org_amt, Usd_amt, Rmb_amt,
                                    Balance, Purpose, Pos_owner, Trans_type, Ip_code, Bord_flag, Nation)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, cred_txn_data)
        print(f"已创建 {len(cred_txn_data)} 条信用卡交易记录")

    def create_cross_border_transactions(self, pers_customers, unit_customers):
        """创建跨境交易记录"""
        print("创建跨境交易记录...")
        cross_border_data = []

        # 选择部分客户进行跨境交易
        all_customers = pers_customers[:6] + unit_customers[:2]  # 6个个人+2个企业
        countries = ['USA', 'GBR', 'JPN', 'HKG', 'SGP', 'AUS', 'CAN', 'DEU']

        for i, cst_data in enumerate(all_customers):
            cst_no = cst_data[2]  # Cst_no
            acc_name = cst_data[5] if len(cst_data) > 5 else cst_data[4]  # Acc_name
            id_no = cst_data[9] if len(cst_data) > 9 else cst_data[6]  # Id_no 或 License

            # 每个客户创建2-5笔跨境交易
            txn_count = random.randint(2, 5)
            for j in range(txn_count):
                date = self.generate_random_date_char8('2024-01-01', '2024-12-31')
                time = self.generate_random_time_char6()

                org_amt = self.generate_random_amount(1000, 500000)
                usd_amt = round(org_amt * random.uniform(0.13, 0.15), 2)
                rmb_amt = org_amt

                cross_border_data.append((
                    date, time,
                    random.choice(['10', '11']),  # Lend_flag
                    random.choice(['10', '11']),  # Tsf_flag
                    random.choice(['11', '12']),  # Unit_flag
                    cst_no, id_no, acc_name,
                    f"6228{random.randint(1000000000000000, 9999999999999999)}",  # Self_acc_no
                    f"6225{random.randint(1000000000000000, 9999999999999999)}",  # Card_no
                    f"北京市朝阳区跨境业务部{i}",  # Self_add
                    f"CB{date}{random.randint(1000, 9999)}",  # Ticd
                    f"FOREIGN{random.randint(1000000000, 9999999999)}",  # Part_acc_no
                    f"海外公司{j+1}",  # Part_acc_name
                    random.choice(countries),  # Part_nation
                    random.choice(['CNY', 'USD']),  # Cur
                    org_amt, usd_amt, rmb_amt,
                    self.generate_random_amount(0, 200000),  # Balance
                    random.choice(['11', '12']),  # Agency_flag
                    None, None, None, None,  # 代理信息
                    random.choice(self.settle_types),  # Settle_type
                    '10',  # Reverse_flag
                    random.choice(['货物贸易', '服务贸易', '投资收益', '个人汇款']),
                    '11',  # Bord_flag
                    random.choice(countries),  # Nation
                    random.choice(['11', '12', '13', '14', '15']),  # Bank_flag
                    f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 255)}",  # Ip_code
                    f"ATM{random.randint(1000, 9999)}",  # Atm_code
                    random.choice(self.bank_codes),  # Bank_code
                    f"IMEI{random.randint(1000000000000000, 9999999999999999)}"  # Mac_info
                ))

        self.cursor.executemany("""
            INSERT INTO tb_cross_border (Date, Time, Lend_flag, Tsf_flag, Unit_flag, Cst_no, Id_no,
                                        Self_acc_name, Self_acc_no, Card_no, Self_add, Ticd, Part_acc_no,
                                        Part_acc_name, Part_nation, Cur, Org_amt, Usd_amt, Rmb_amt,
                                        Balance, Agency_flag, Agent_name, Agent_tel, Agent_type, Agent_no,
                                        Settle_type, Reverse_flag, Purpose, Bord_flag, Nation, Bank_flag,
                                        Ip_code, Atm_code, Bank_code, Mac_info)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, cross_border_data)
        print(f"已创建 {len(cross_border_data)} 条跨境交易记录")

    def create_cash_transactions(self, pers_customers):
        """创建现金交易记录"""
        print("创建现金交易记录...")

        # 现金汇款
        cash_remit_data = []
        customers = pers_customers[:8]  # 选择8个个人客户

        for i, cst_data in enumerate(customers):
            cst_name = cst_data[5]  # Acc_name
            id_no = cst_data[9]  # Id_no

            # 每个客户创建2-4笔现金汇款
            remit_count = random.randint(2, 4)
            for j in range(remit_count):
                date = self.generate_random_date_char8('2024-01-01', '2024-12-31')
                time = self.generate_random_time_char6()

                org_amt = self.generate_random_amount(1000, 100000)
                usd_amt = round(org_amt * random.uniform(0.13, 0.15), 2)
                rmb_amt = org_amt

                cash_remit_data.append((
                    date, time,
                    random.choice(self.bank_codes),  # Self_bank_code
                    cst_name, id_no,
                    random.choice(['CNY', 'USD']),  # Cur
                    org_amt, usd_amt, rmb_amt,
                    random.choice(['工商银行', '建设银行', '招商银行', '民生银行']),  # Part_bank
                    f"6228{random.randint(1000000000000000, 9999999999999999)}",  # Part_acc_no
                    f"现金收款人{j+1}",  # Part_acc_name
                    random.choice(self.settle_types),  # Settle_type
                    f"CR{date}{random.randint(1000, 9999)}"  # Ticd
                ))

        self.cursor.executemany("""
            INSERT INTO tb_cash_remit (Date, Time, Self_bank_code, Acc_name, Id_no,
                                      Cur, Org_amt, Usd_amt, Rmb_amt, Part_bank,
                                      Part_acc_no, Part_acc_name, Settle_type, Ticd)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, cash_remit_data)
        print(f"已创建 {len(cash_remit_data)} 条现金汇款记录")

        # 现钞结售汇
        cash_convert_data = []
        for i, cst_data in enumerate(customers[:6]):  # 选择6个客户
            cst_name = cst_data[5]  # Acc_name
            id_no = cst_data[9]  # Id_no

            # 每个客户创建1-3笔现钞结售汇
            convert_count = random.randint(1, 3)
            for j in range(convert_count):
                date = self.generate_random_date_char8('2024-01-01', '2024-12-31')
                time = self.generate_random_time_char6()

                out_cur, in_cur = random.sample(['CNY', 'USD', 'EUR', 'JPY'], 2)
                out_amt = self.generate_random_amount(100, 50000)
                out_usd = round(out_amt * random.uniform(0.13, 0.15), 2) if out_cur != 'USD' else out_amt
                in_amt = self.generate_random_amount(100, 50000)
                in_usd = round(in_amt * random.uniform(0.13, 0.15), 2) if in_cur != 'USD' else in_amt

                cash_convert_data.append((
                    date, time,
                    random.choice(self.bank_codes),  # Self_bank_code
                    cst_name, id_no,
                    out_cur, out_amt, out_usd,
                    in_cur, in_amt, in_usd,
                    f"CC{date}{random.randint(1000, 9999)}",  # Ticd
                    f"CT{random.randint(1000, 9999)}",  # Counter_no
                    random.choice(self.settle_types)  # Settle_type
                ))

        self.cursor.executemany("""
            INSERT INTO tb_cash_convert (Date, Time, Self_bank_code, Acc_name, Id_no,
                                        Out_cur, Out_org_amt, Out_usd_amt,
                                        In_cur, In_org_amt, In_usd_amt,
                                        Ticd, Counter_no, Settle_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, cash_convert_data)
        print(f"已创建 {len(cash_convert_data)} 条现钞结售汇记录")

    def create_lwhc_logs(self, pers_customers):
        """创建公民联网核查日志"""
        print("创建公民联网核查日志...")

        lwhc_log_data = []
        customers = pers_customers[:10]  # 所有个人客户

        # 获取银行信息
        self.cursor.execute("SELECT Bank_name, Bank_code1 FROM tb_bank")
        banks = self.cursor.fetchall()

        for i, cst_data in enumerate(customers):
            cst_name = cst_data[5]  # Acc_name
            id_no = cst_data[9]  # Id_no

            # 每个客户创建2-5条核查记录
            log_count = random.randint(2, 5)
            for j in range(log_count):
                bank_name, bank_code = random.choice(banks)
                date = self.generate_random_date_char8('2024-01-01', '2024-12-31')
                time = self.generate_random_time_char6()

                # 核查结果：11-16
                result = random.choice(['11', '12', '13', '14', '15', '16'])

                lwhc_log_data.append((
                    bank_name, bank_code, date, time, cst_name, id_no, result,
                    f"CT{random.randint(1000, 9999)}",
                    random.choice(['个人金融', '公司业务', '信用卡业务', '理财业务']),
                    random.choice(['10', '11']),  # Mode
                    random.choice(['开户核查', '变更核查', '业务办理核查'])
                ))

        self.cursor.executemany("""
            INSERT INTO tb_lwhc_log (Bank_name, Bank_code2, Date, Time, Name, Id_no,
                                     Result, Counter_no, Ope_line, Mode, Purpose)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, lwhc_log_data)
        print(f"已创建 {len(lwhc_log_data)} 条联网核查日志记录")

    def create_reports(self, pers_customers, unit_customers):
        """创建大额和可疑交易报告"""
        print("创建大额和可疑交易报告...")

        # 获取银行信息
        self.cursor.execute("SELECT Bank_name FROM tb_bank")
        bank_names = [row[0] for row in self.cursor.fetchall()]

        # 大额交易报告
        lar_report_data = []
        # 为部分交易生成大额报告
        for i in range(15):  # 生成15条大额报告
            report_date = self.generate_random_date_char8('2024-01-01', '2024-12-31')
            trans_date = self.generate_random_date_char8('2024-01-01', '2024-12-31')

            lar_report_data.append((
                random.choice(['00', '01', '02']),  # RLFC
                None,  # ROTF
                f"RPM{random.randint(1000000000000000, 9999999999999999)}",  # RPMN
                None,  # RPMT
                report_date,
                random.choice(bank_names),
                self.generate_random_amount(500000, 50000000),  # 大额
                random.choice(['CNY', 'USD']),
                random.choice(['现金存款', '现金取款', '转账', '跨境汇款']),
                trans_date,
                random.choice([cst[5] for cst in pers_customers[:5]] + [cst[4] for cst in unit_customers[:2]]),  # Customer_Name
                random.choice([cst[9] for cst in pers_customers[:5]] + [cst[6] for cst in unit_customers[:2]]),  # Customer_ID
                f"6228{random.randint(1000000000000000, 9999999999999999)}"
            ))

        self.cursor.executemany("""
            INSERT INTO tb_lar_report (RLFC, ROTF, RPMN, RPMT, Report_Date,
                                      Institution_Name, Report_Amount, Currency,
                                      Transaction_Type, Transaction_Date,
                                      Customer_Name, Customer_ID, Account_No)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, lar_report_data)
        print(f"已创建 {len(lar_report_data)} 条大额交易报告记录")

        # 可疑交易报告
        sus_report_data = []
        suspicious_reasons = [
            '交易金额与客户身份不符', '频繁的大额现金交易', '跨境交易异常',
            '交易模式异常', '涉及高风险地区', '短期内多笔大额交易',
            '与制裁名单相关', '交易时间异常', '交易对手风险高'
        ]

        for i in range(8):  # 生成8条可疑报告
            report_date = self.generate_random_date_char8('2024-01-01', '2024-12-31')
            report_time = f"{random.randint(8, 18):02d}{random.randint(0, 59):02d}"

            sus_report_data.append((
                None, None, None, None, None, None, None, None, None,  # 交易代办人信息（可为空）
                f"TX{report_date}{random.randint(1000, 9999)}",  # TICD
                'CHN000000',  # TRCD
                report_date,
                random.choice(bank_names),
                self.generate_random_amount(100000, 10000000),
                random.choice(['CNY', 'USD']),
                random.choice(['洗钱风险', '恐怖融资', '欺诈交易', '其他可疑']),
                random.choice(suspicious_reasons),
                report_time
            ))

        self.cursor.executemany("""
            INSERT INTO tb_sus_report (TBID, TBIT, TBNM, TBNT, TCAC, TCAT, TCID, TCIT, TCNM, TICD, TRCD,
                                      Report_Date, Institution_Name, Transaction_Amount, Currency,
                                      Transaction_Type, Suspicious_Reason, Report_Time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, sus_report_data)
        print(f"已创建 {len(sus_report_data)} 条可疑交易报告记录")

    def generate_all_test_data(self):
        """生成所有测试数据"""
        try:
            print("=== 开始生成完整的15张表测试数据 ===\n")
            print("数据规模: 10个个人客户 + 2个企业客户 + 完整交易/风险/报告数据\n")

            # 清理现有数据
            self.clear_existing_data()

            # 按依赖关系顺序创建数据
            print("1. 创建基础客户数据...")
            pers_customers = self.create_individual_customers(10)
            unit_customers = self.create_corporate_customers(2)

            print("\n2. 创建账户数据...")
            accounts = self.create_accounts(pers_customers, unit_customers)

            print("\n3. 创建风险等级数据...")
            self.create_risk_levels(pers_customers, unit_customers)

            print("\n4. 创建交易数据...")
            self.create_account_transactions(accounts)
            self.create_credit_transactions(pers_customers)
            self.create_cross_border_transactions(pers_customers, unit_customers)
            self.create_cash_transactions(pers_customers)

            print("\n5. 创建核查和报告数据...")
            self.create_lwhc_logs(pers_customers)
            self.create_reports(pers_customers, unit_customers)

            # 提交事务
            self.conn.commit()
            print(f"\n[SUCCESS] 完整的15张表测试数据生成成功！")

            # 统计生成的记录数
            self.cursor.execute("SHOW TABLES")
            tables = [table[0] for table in self.cursor.fetchall()]

            print(f"\n各表记录数统计:")
            total_records = 0
            for table in tables:
                try:
                    self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = self.cursor.fetchone()[0]
                    total_records += count
                    print(f"  {table:15} : {count:8d} 条记录")
                except:
                    print(f"  {table:15} : 查询失败")

            print(f"\n总记录数: {total_records:,}")
            print(f"\n✅ 数据覆盖情况:")
            print(f"  - 个人客户: {len(pers_customers)} 个")
            print(f"  - 企业客户: {len(unit_customers)} 个")
            print(f"  - 账户: {len(accounts)} 个")
            print(f"  - 银行信息: 使用现有 {len(self.bank_codes)} 个银行")
            print(f"  - 业务类型: 使用现有 {len(self.settle_types)} 种类型")

            return True

        except Exception as e:
            print(f"[ERROR] 生成测试数据失败: {e}")
            if 'conn' in locals():
                self.conn.rollback()
            return False

    def close(self):
        """关闭数据库连接"""
        self.cursor.close()
        self.conn.close()

if __name__ == "__main__":
    generator = ComprehensiveTestDataGenerator()
    success = generator.generate_all_test_data()
    generator.close()
    sys.exit(0 if success else 1)