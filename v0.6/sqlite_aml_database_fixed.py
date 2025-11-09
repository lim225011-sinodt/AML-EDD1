#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AML-EDD反洗钱数据库SQLite演示程序
版本: v1.0
创建时间: 2025-11-09
功能: 使用SQLite演示AML-EDD数据库结构和数据生成
"""

import sqlite3
import random
import string
import time
import os
from datetime import datetime, timedelta

class AMLDatabaseGenerator:
    def __init__(self, db_path='aml_edd_demo.db'):
        self.db_path = db_path
        self.connection = None
        self.cursor = None

    def connect(self):
        """连接SQLite数据库"""
        try:
            # 如果数据库文件已存在，先删除
            if os.path.exists(self.db_path):
                os.remove(self.db_path)
                print(f"[INFO] 删除旧数据库文件: {self.db_path}")

            self.connection = sqlite3.connect(self.db_path)
            self.cursor = self.connection.cursor()
            # 启用外键约束
            self.cursor.execute("PRAGMA foreign_keys = ON")
            print(f"[OK] 成功连接到SQLite数据库: {self.db_path}")
            return True
        except Exception as e:
            print(f"[ERROR] 连接数据库失败: {e}")
            return False

    def create_tables(self):
        """创建所有表结构"""
        print("开始创建数据表...")

        # 1. 机构对照表
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS tb_bank (
                Head_no TEXT NOT NULL,
                Bank_code1 TEXT PRIMARY KEY,
                Bank_code2 TEXT UNIQUE,
                Bank_name TEXT NOT NULL,
                Bord_type TEXT NOT NULL DEFAULT '10'
            )
        ''')

        # 2. 业务类型对照表
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS tb_settle_type (
                Head_no TEXT NOT NULL,
                Settle_type TEXT PRIMARY KEY,
                Name TEXT NOT NULL
            )
        ''')

        # 3. 个人客户信息表
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS tb_cst_pers (
                Head_no TEXT NOT NULL,
                Bank_code1 TEXT NOT NULL,
                Cst_no TEXT PRIMARY KEY,
                Open_time TEXT NOT NULL,
                Close_time TEXT,
                Acc_name TEXT NOT NULL,
                Cst_sex TEXT NOT NULL,
                Nation TEXT NOT NULL,
                Id_type TEXT NOT NULL,
                Id_no TEXT NOT NULL,
                Id_deadline TEXT NOT NULL,
                Occupation TEXT,
                Income REAL,
                Contact1 TEXT,
                Contact2 TEXT,
                Contact3 TEXT,
                Address1 TEXT,
                Address2 TEXT,
                Address3 TEXT,
                Company TEXT,
                Sys_name TEXT,
                FOREIGN KEY (Bank_code1) REFERENCES tb_bank(Bank_code1)
            )
        ''')

        # 4. 企业客户信息表
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS tb_cst_unit (
                Head_no TEXT NOT NULL,
                Bank_code1 TEXT NOT NULL,
                Cst_no TEXT PRIMARY KEY,
                Open_time TEXT NOT NULL,
                Acc_name TEXT NOT NULL,
                Rep_name TEXT,
                Ope_name TEXT,
                License TEXT,
                Id_deadline TEXT,
                Industry TEXT,
                Reg_amt REAL,
                Reg_amt_code TEXT,
                Sys_name TEXT,
                FOREIGN KEY (Bank_code1) REFERENCES tb_bank(Bank_code1)
            )
        ''')

        # 5. 账户主档
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS tb_acc (
                Head_no TEXT NOT NULL,
                Bank_code1 TEXT NOT NULL,
                Self_acc_name TEXT NOT NULL,
                Acc_state TEXT NOT NULL DEFAULT '11',
                Self_acc_no TEXT NOT NULL,
                Card_no TEXT,
                Acc_type TEXT NOT NULL,
                Acc_type1 TEXT,
                Id_no TEXT NOT NULL,
                Cst_no TEXT NOT NULL,
                Open_time TEXT NOT NULL,
                Close_time TEXT,
                Agency_flag TEXT,
                Acc_flag TEXT,
                Fixed_flag TEXT,
                PRIMARY KEY (Self_acc_no, Card_no),
                FOREIGN KEY (Bank_code1) REFERENCES tb_bank(Bank_code1)
            )
        ''')

        # 6. 交易记录
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS tb_acc_txn (
                Date TEXT NOT NULL,
                Time TEXT NOT NULL,
                Self_bank_code TEXT NOT NULL,
                Acc_type TEXT NOT NULL,
                Cst_no TEXT NOT NULL,
                Id_no TEXT NOT NULL,
                Self_acc_no TEXT NOT NULL,
                Card_no TEXT,
                Part_acc_no TEXT,
                Part_acc_name TEXT,
                Lend_flag TEXT NOT NULL,
                Tsf_flag TEXT NOT NULL,
                Cur TEXT NOT NULL,
                Org_amt REAL NOT NULL,
                Usd_amt REAL NOT NULL,
                Rmb_amt REAL NOT NULL,
                Balance REAL,
                Agency_flag TEXT,
                Agent_name TEXT,
                Agent_tel TEXT,
                Agent_type TEXT,
                Agent_no TEXT,
                Reverse_flag TEXT NOT NULL DEFAULT '10',
                Purpose TEXT,
                Bord_flag TEXT DEFAULT '12',
                Nation TEXT,
                Bank_flag TEXT,
                Ip_code TEXT,
                Atm_code TEXT,
                Bank_code TEXT,
                Mac_info TEXT,
                Settle_type TEXT,
                Ticd TEXT,
                PRIMARY KEY (Date, Time, Self_acc_no, Lend_flag, Tsf_flag),
                FOREIGN KEY (Self_bank_code) REFERENCES tb_bank(Bank_code1)
            )
        ''')

        # 7. 风险等级历史
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS tb_risk_his (
                Bank_code1 TEXT NOT NULL,
                Cst_no TEXT NOT NULL,
                Self_acc_name TEXT,
                Id_no TEXT NOT NULL,
                Acc_type TEXT NOT NULL,
                Risk_code TEXT NOT NULL,
                Time TEXT NOT NULL,
                Norm TEXT,
                PRIMARY KEY (Cst_no, Time),
                FOREIGN KEY (Bank_code1) REFERENCES tb_bank(Bank_code1)
            )
        ''')

        # 8. 最新风险等级
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS tb_risk_new (
                Bank_code1 TEXT NOT NULL,
                Cst_no TEXT PRIMARY KEY,
                Self_acc_name TEXT,
                Id_no TEXT NOT NULL,
                Acc_type TEXT NOT NULL,
                Risk_code TEXT NOT NULL,
                Time TEXT NOT NULL,
                Norm TEXT,
                FOREIGN KEY (Bank_code1) REFERENCES tb_bank(Bank_code1)
            )
        ''')

        self.connection.commit()
        print("[OK] 所有数据表创建完成")
        return True

    def generate_bank_data(self):
        """生成机构基础数据"""
        print("生成机构基础数据...")

        banks = [
            ('ABC001', '103100000019', '104100000004', '中国农业银行总行营业部', '10'),
            ('ABC001', '103100000027', '104100000012', '中国农业银行北京分行营业部', '10'),
            ('ABC001', '103100000035', '104100000020', '中国农业银行上海分行营业部', '10'),
            ('ABC001', '103100000043', '104100000038', '中国农业银行广东分行营业部', '10'),
            ('ABC001', '103100000050', '104100000045', '中国农业银行深圳分行营业部', '10'),
        ]

        self.cursor.executemany(
            "INSERT OR REPLACE INTO tb_bank (Head_no, Bank_code1, Bank_code2, Bank_name, Bord_type) VALUES (?, ?, ?, ?, ?)",
            banks
        )

        self.connection.commit()
        print(f"[OK] 机构数据生成完成: {len(banks)} 条记录")

    def generate_settle_type_data(self):
        """生成业务类型数据"""
        print("生成业务类型数据...")

        settle_types = [
            ('ABC001', 'ST001', '存款业务'),
            ('ABC001', 'ST002', '取款业务'),
            ('ABC001', 'ST003', '转账业务'),
            ('ABC001', 'ST004', '汇款业务'),
            ('ABC001', 'ST005', '消费业务'),
            ('ABC001', 'ST006', '代收代付'),
            ('ABC001', 'ST007', '贷款发放'),
            ('ABC001', 'ST008', '贷款还款'),
            ('ABC001', 'ST009', '投资理财'),
            ('ABC001', 'ST010', '外汇买卖'),
        ]

        self.cursor.executemany(
            "INSERT OR REPLACE INTO tb_settle_type (Head_no, Settle_type, Name) VALUES (?, ?, ?)",
            settle_types
        )

        self.connection.commit()
        print(f"[OK] 业务类型数据生成完成: {len(settle_types)} 条记录")

    def generate_chinese_name(self):
        """生成中文名字"""
        first_names = ['李', '王', '张', '刘', '陈', '杨', '赵', '黄', '周', '吴',
                      '徐', '孙', '胡', '朱', '高', '林', '何', '郭', '马', '罗']
        last_names = ['伟', '芳', '娜', '秀英', '敏', '静', '丽', '强', '磊', '军',
                     '洋', '勇', '艳', '杰', '娟', '涛', '明', '超', '秀兰', '霞',
                     '平', '红', '英', '华', '文', '建华', '志强', '建军', '国强', '国庆']

        return random.choice(first_names) + random.choice(last_names)

    def generate_id_card(self):
        """生成身份证号码"""
        area_code = random.choice(['110101', '310101', '440103', '440304', '330102'])
        birth_year = random.randint(1960, 2000)
        birth_month = random.randint(1, 12)
        birth_day = random.randint(1, 28)
        sequence = random.randint(100, 999)
        check_code = random.choice('0123456789X')

        return f"{area_code}{birth_year:04d}{birth_month:02d}{birth_day:02d}{sequence}{check_code}"

    def generate_phone_number(self):
        """生成手机号码"""
        prefixes = ['130', '131', '132', '133', '134', '135', '136', '137', '138', '139',
                   '150', '151', '152', '153', '155', '156', '157', '158', '159', '180',
                   '181', '182', '183', '184', '185', '186', '187', '188', '189']

        prefix = random.choice(prefixes)
        suffix = ''.join([str(random.randint(0, 9)) for _ in range(8)])

        return prefix + suffix

    def generate_company_name(self):
        """生成公司名称"""
        prefixes = ['北京', '上海', '广州', '深圳', '杭州', '南京', '武汉', '成都', '西安', '重庆']
        names = ['华强', '金鼎', '华润', '中信', '光大', '招商', '民生', '平安', '华为', '腾讯']
        types = ['科技有限公司', '贸易有限公司', '实业有限公司', '投资有限公司', '咨询有限公司']

        return random.choice(prefixes) + random.choice(names) + random.choice(types)

    def generate_credit_code(self):
        """生成统一社会信用代码"""
        code = ''.join([random.choice('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ') for _ in range(17)])
        return code + random.choice('0123456789')

    def generate_person_customers(self, count=100):
        """生成个人客户数据（为了演示，减少到100条）"""
        print(f"生成个人客户数据... {count} 条")

        customers = []
        for i in range(count):
            open_date = datetime.now() - timedelta(days=random.randint(365, 3650))

            customer = (
                'ABC001',  # Head_no
                random.choice(['103100000019', '103100000027', '103100000035', '103100000043', '103100000050']),  # Bank_code1
                f'P{i+1:06d}',  # Cst_no
                open_date.strftime('%Y%m%d'),  # Open_time
                None if random.random() > 0.9 else (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y%m%d'),  # Close_time
                self.generate_chinese_name(),  # Acc_name
                random.choice(['11', '12']),  # Cst_sex (11男, 12女)
                'CHN',  # Nation
                '11',  # Id_type (身份证)
                self.generate_id_card(),  # Id_no
                '99991231' if random.random() > 0.8 else (datetime.now() + timedelta(days=random.randint(365, 3650))).strftime('%Y%m%d'),  # Id_deadline
                random.choice(['软件工程师', '教师', '医生', '律师', '会计', '销售经理', '企业高管', '公务员', '工程师', '设计师']),  # Occupation
                round(random.uniform(50000, 500000), 2),  # Income
                self.generate_phone_number(),  # Contact1
                self.generate_phone_number() if random.random() > 0.5 else None,  # Contact2
                self.generate_phone_number() if random.random() > 0.7 else None,  # Contact3
                f'{random.choice(["北京市", "上海市", "广州市", "深圳市", "杭州市"])}{random.choice(["朝阳区", "海淀区", "西城区"])}{random.choice(["金融街", "CBD", "高新区"])}{random.randint(1, 100)}号',  # Address1
                f'{random.choice(["北京市", "上海市"])}临时地址' if random.random() > 0.6 else None,  # Address2
                f'{random.choice(["广州市", "深圳市"])}备用地址' if random.random() > 0.8 else None,  # Address3
                self.generate_company_name() if random.random() > 0.4 else None,  # Company
                random.choice(['个人网银系统', '柜面系统'])  # Sys_name
            )
            customers.append(customer)

        self.cursor.executemany(
            """
            INSERT INTO tb_cst_pers (
                Head_no, Bank_code1, Cst_no, Open_time, Close_time, Acc_name,
                Cst_sex, Nation, Id_type, Id_no, Id_deadline, Occupation, Income,
                Contact1, Contact2, Contact3, Address1, Address2, Address3, Company, Sys_name
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            customers
        )

        self.connection.commit()
        print(f"[OK] 个人客户数据生成完成: {len(customers)} 条记录")

    def generate_unit_customers(self, count=20):
        """生成企业客户数据（为了演示，减少到20条）"""
        print(f"生成企业客户数据... {count} 条")

        companies = []
        for i in range(count):
            open_date = datetime.now() - timedelta(days=random.randint(365, 3650))

            company = (
                'ABC001',  # Head_no
                random.choice(['103100000019', '103100000027', '103100000035', '103100000043', '103100000050']),  # Bank_code1
                f'U{i+1:04d}',  # Cst_no
                open_date.strftime('%Y%m%d'),  # Open_time
                self.generate_company_name(),  # Acc_name
                self.generate_chinese_name(),  # Rep_name
                self.generate_chinese_name() if random.random() > 0.3 else None,  # Ope_name
                self.generate_credit_code(),  # License
                '99991231' if random.random() > 0.7 else (datetime.now() + timedelta(days=random.randint(365, 3650))).strftime('%Y%m%d'),  # Id_deadline
                random.choice(['软件开发', '电子商务', '金融服务', '教育培训', '医疗健康', '房地产', '建筑工程', '制造业', '批发零售', '物流运输']),  # Industry
                round(random.uniform(1000000, 50000000), 2),  # Reg_amt
                random.choice(['RMB', 'USD', 'EUR']),  # Reg_amt_code
                random.choice(['企业网银系统', '对公业务系统'])  # Sys_name
            )
            companies.append(company)

        self.cursor.executemany(
            """
            INSERT INTO tb_cst_unit (
                Head_no, Bank_code1, Cst_no, Open_time, Acc_name,
                Rep_name, Ope_name, License, Id_deadline, Industry,
                Reg_amt, Reg_amt_code, Sys_name
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            companies
        )

        self.connection.commit()
        print(f"[OK] 企业客户数据生成完成: {len(companies)} 条记录")

    def generate_accounts(self):
        """生成账户数据"""
        print("生成账户数据...")

        accounts = []

        # 为个人客户生成账户
        self.cursor.execute("SELECT Cst_no, Acc_name, Id_no, Bank_code1, Open_time FROM tb_cst_pers WHERE RANDOM() < 0.8")
        person_customers = self.cursor.fetchall()

        for cst_no, acc_name, id_no, bank_code1, open_time in person_customers:
            account = (
                'ABC001',  # Head_no
                bank_code1,  # Bank_code1
                acc_name,  # Self_acc_name
                '11',  # Acc_state (正常)
                f'ACC{cst_no[1:].zfill(8)}',  # Self_acc_no
                f'CARD{cst_no[1:].zfill(8)}' if random.random() > 0.3 else None,  # Card_no
                '11',  # Acc_type (个人)
                random.choice(['11', '12', '13', '14']),  # Acc_type1 (I类, II类, III类, 信用卡)
                id_no,  # Id_no
                cst_no,  # Cst_no
                open_time,  # Open_time
                None if random.random() > 0.95 else (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y%m%d'),  # Close_time
                random.choice(['11', '12']),  # Agency_flag
                '11' if random.random() > 0.8 else '12',  # Acc_flag
                '11' if random.random() > 0.9 else '12'  # Fixed_flag
            )
            accounts.append(account)

        # 为企业客户生成账户
        self.cursor.execute("SELECT Cst_no, Acc_name, License, Bank_code1, Open_time FROM tb_cst_unit")
        unit_customers = self.cursor.fetchall()

        for cst_no, acc_name, license, bank_code1, open_time in unit_customers:
            account = (
                'ABC001',  # Head_no
                bank_code1,  # Bank_code1
                acc_name,  # Self_acc_name
                '11',  # Acc_state (正常)
                f'BIZ{cst_no[1:].zfill(6)}',  # Self_acc_no
                f'BCARD{cst_no[1:].zfill(6)}' if random.random() > 0.5 else None,  # Card_no
                '12',  # Acc_type (单位)
                None,  # Acc_type1
                license,  # Id_no
                cst_no,  # Cst_no
                open_time,  # Open_time
                None if random.random() > 0.95 else (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y%m%d'),  # Close_time
                random.choice(['11', '12']),  # Agency_flag
                '11' if random.random() > 0.7 else '12',  # Acc_flag
                '11' if random.random() > 0.8 else '12'  # Fixed_flag
            )
            accounts.append(account)

        self.cursor.executemany(
            """
            INSERT INTO tb_acc (
                Head_no, Bank_code1, Self_acc_name, Acc_state, Self_acc_no,
                Card_no, Acc_type, Acc_type1, Id_no, Cst_no, Open_time,
                Close_time, Agency_flag, Acc_flag, Fixed_flag
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            accounts
        )

        self.connection.commit()
        print(f"[OK] 账户数据生成完成: {len(accounts)} 条记录")

    def generate_transactions(self, count=1000):
        """生成交易数据（为了演示，减少到1000条）"""
        print(f"生成交易数据... {count} 条")

        transactions = []
        purposes = ['工资发放', '采购付款', '货款结算', '服务费', '租金支付', '投资收益', '还款', '转账', '消费', '提现',
                   '报销费用', '捐赠', '保险费', '税费', '分红', '货款', '服务款', '咨询费', '差旅费', '其他费用']

        # 获取所有账户
        self.cursor.execute("SELECT Self_acc_no, Card_no, Cst_no, Id_no, Acc_type, Bank_code1 FROM tb_acc WHERE Acc_state = '11'")
        all_accounts = self.cursor.fetchall()

        for i in range(count):
            acc_no, card_no, cst_no, id_no, acc_type, bank_code = random.choice(all_accounts)

            # 生成随机交易时间（最近90天内）
            trans_date = datetime.now() - timedelta(days=random.randint(0, 90))
            trans_time = datetime.now().replace(
                hour=random.randint(0, 23),
                minute=random.randint(0, 59),
                second=random.randint(0, 59)
            )

            # 随机交易参数
            currency = random.choice(['CNY', 'USD', 'EUR'])
            amount = round(random.uniform(100, 100000), 2)

            # 货币转换（简化）
            if currency == 'CNY':
                usd_amount = amount / 7.0
                rmb_amount = amount
            elif currency == 'USD':
                usd_amount = amount
                rmb_amount = amount * 7.0
            else:  # EUR
                usd_amount = amount * 1.1
                rmb_amount = amount * 7.7

            transaction = (
                trans_date.strftime('%Y%m%d'),  # Date
                trans_time.strftime('%H%M%S'),  # Time
                bank_code,  # Self_bank_code
                acc_type,  # Acc_type
                cst_no,  # Cst_no
                id_no,  # Id_no
                acc_no,  # Self_acc_no
                card_no,  # Card_no
                f'PART{str(random.randint(100000, 999999)).zfill(6)}' if random.random() > 0.3 else None,  # Part_acc_no
                self.generate_chinese_name() if random.random() > 0.4 else None,  # Part_acc_name
                random.choice(['10', '11']),  # Lend_flag (收/付)
                random.choice(['10', '11']),  # Tsf_flag (现金/转账)
                currency,  # Cur
                amount,  # Org_amt
                usd_amount,  # Usd_amt
                rmb_amount,  # Rmb_amt
                round(random.uniform(10000, 1000000), 2),  # Balance
                random.choice(['11', '12']),  # Agency_flag
                '10',  # Reverse_flag
                random.choice(purposes),  # Purpose
                '11' if random.random() > 0.9 else '12',  # Bord_flag (跨境标识)
                random.choice(['11', '12', '13', '14', '15']),  # Bank_flag (交易方式)
                f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}" if random.random() > 0.5 else None,  # Ip_code
                None,  # Mac_info
                f'ST{str(random.randint(1, 10)).zfill(3)}',  # Settle_type
                f'TXN{trans_date.strftime("%Y%m%d")}{str(i+1).zfill(6)}'  # Ticd
            )
            transactions.append(transaction)

            # 每100条提交一次
            if len(transactions) >= 100:
                self.cursor.executemany(
                    """
                    INSERT INTO tb_acc_txn (
                        Date, Time, Self_bank_code, Acc_type, Cst_no, Id_no,
                        Self_acc_no, Card_no, Part_acc_no, Part_acc_name, Lend_flag,
                        Tsf_flag, Cur, Org_amt, Usd_amt, Rmb_amt, Balance,
                        Agency_flag, Reverse_flag, Purpose, Bord_flag, Bank_flag,
                        Ip_code, Mac_info, Settle_type, Ticd
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    transactions
                )
                self.connection.commit()
                print(f"  已插入 {i+1} 条交易记录...")
                transactions = []

        # 插入剩余的交易记录
        if transactions:
            self.cursor.executemany(
                """
                INSERT INTO tb_acc_txn (
                    Date, Time, Self_bank_code, Acc_type, Cst_no, Id_no,
                    Self_acc_no, Card_no, Part_acc_no, Part_acc_name, Lend_flag,
                    Tsf_flag, Cur, Org_amt, Usd_amt, Rmb_amt, Balance,
                    Agency_flag, Reverse_flag, Purpose, Bord_flag, Bank_flag,
                    Ip_code, Mac_info, Settle_type, Ticd
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                transactions
            )
            self.connection.commit()

        print(f"[OK] 交易数据生成完成: {count} 条记录")

    def generate_risk_levels(self):
        """生成风险等级数据"""
        print("生成风险等级数据...")

        # 为个人客户生成最新风险等级
        self.cursor.execute("SELECT Bank_code1, Cst_no, Acc_name, Id_no FROM tb_cst_pers")
        person_customers = self.cursor.fetchall()

        person_risks = []
        for bank_code, cst_no, acc_name, id_no in person_customers:
            risk = (
                bank_code,  # Bank_code1
                cst_no,  # Cst_no
                acc_name,  # Self_acc_name
                id_no,  # Id_no
                '11',  # Acc_type (个人)
                random.choice(['10', '11', '12', '13']),  # Risk_code (风险等级)
                (datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y%m%d'),  # Time
                random.choice(['客户交易金额较大，资金往来频繁', '客户行业属于高风险行业', '客户为新开户客户', '客户资金来源不明，需要持续监控', '客户综合评分正常，风险较低'])  # Norm
            )
            person_risks.append(risk)

        self.cursor.executemany(
            "INSERT INTO tb_risk_new (Bank_code1, Cst_no, Self_acc_name, Id_no, Acc_type, Risk_code, Time, Norm) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            person_risks
        )

        # 为企业客户生成最新风险等级
        self.cursor.execute("SELECT Bank_code1, Cst_no, Acc_name, License FROM tb_cst_unit")
        unit_customers = self.cursor.fetchall()

        unit_risks = []
        for bank_code, cst_no, acc_name, license in unit_customers:
            risk = (
                bank_code,  # Bank_code1
                cst_no,  # Cst_no
                acc_name,  # Self_acc_name
                license,  # Id_no
                '12',  # Acc_type (单位)
                random.choice(['10', '11', '12', '13']),  # Risk_code (风险等级)
                (datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y%m%d'),  # Time
                random.choice(['企业注册资本较高，交易规模大', '企业所属行业风险等级中等', '企业经营状况稳定，风险可控', '新成立企业，需要加强监控', '企业合规情况良好，风险较低'])  # Norm
            )
            unit_risks.append(risk)

        self.cursor.executemany(
            "INSERT INTO tb_risk_new (Bank_code1, Cst_no, Self_acc_name, Id_no, Acc_type, Risk_code, Time, Norm) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            unit_risks
        )

        # 为部分客户生成历史风险等级
        all_customers = person_risks + unit_risks
        historical_risks = []

        for i, (bank_code, cst_no, acc_name, id_no, acc_type, _, _, _) in enumerate(all_customers):
            if random.random() > 0.6:  # 40%的客户有历史风险记录
                historical_time = (datetime.now() - timedelta(days=random.randint(365, 730))).strftime('%Y%m%d')
                historical_risk = (
                    bank_code,  # Bank_code1
                    cst_no,  # Cst_no
                    acc_name,  # Self_acc_name
                    id_no,  # Id_no
                    acc_type,  # Acc_type
                    random.choice(['10', '11', '12', '13']),  # Risk_code
                    historical_time,  # Time
                    random.choice(['前期风险评估结果', '定期风险复评结果', '风险等级调整记录'])  # Norm
                )
                historical_risks.append(historical_risk)

        if historical_risks:
            self.cursor.executemany(
                "INSERT INTO tb_risk_his (Bank_code1, Cst_no, Self_acc_name, Id_no, Acc_type, Risk_code, Time, Norm) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                historical_risks
            )

        self.connection.commit()
        print(f"[OK] 最新风险等级数据生成完成: {len(person_risks) + len(unit_risks)} 条记录")
        print(f"[OK] 历史风险等级数据生成完成: {len(historical_risks)} 条记录")

    def generate_statistics_report(self):
        """生成统计报告"""
        print("\n" + "="*50)
        print("数据统计报告")
        print("="*50)

        tables = [
            ('tb_bank', '机构对照表'),
            ('tb_settle_type', '业务类型对照表'),
            ('tb_cst_pers', '个人客户信息'),
            ('tb_cst_unit', '企业客户信息'),
            ('tb_acc', '账户信息'),
            ('tb_acc_txn', '交易记录'),
            ('tb_risk_new', '最新风险等级'),
            ('tb_risk_his', '历史风险等级')
        ]

        for table_name, description in tables:
            try:
                self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = self.cursor.fetchone()[0]
                print(f"{description:15} : {count:6d} 条记录")
            except Exception as e:
                print(f"{description:15} : 查询失败 - {e}")

        # 风险等级分布统计
        print("\n风险等级分布:")
        try:
            self.cursor.execute("""
                SELECT
                    CASE Risk_code
                        WHEN '10' THEN '高风险'
                        WHEN '11' THEN '中高风险'
                        WHEN '12' THEN '中等风险'
                        WHEN '13' THEN '低风险'
                    END as risk_level,
                    Acc_type,
                    COUNT(*) as count
                FROM tb_risk_new
                GROUP BY Risk_code, Acc_type
                ORDER BY Risk_code, Acc_type
            """)
            risk_stats = self.cursor.fetchall()
            for risk_level, acc_type, count in risk_stats:
                acc_type_desc = '个人客户' if acc_type == '11' else '企业客户'
                print(f"  {risk_level:8} {acc_type_desc:8} : {count:4d} 个客户")
        except Exception as e:
            print(f"风险等级统计失败: {e}")

        # 交易统计
        print("\n交易统计:")
        try:
            self.cursor.execute("""
                SELECT
                    COUNT(*) as total_count,
                    ROUND(AVG(Org_amt), 2) as avg_amount,
                    ROUND(MAX(Org_amt), 2) as max_amount,
                    ROUND(MIN(Org_amt), 2) as min_amount,
                    ROUND(SUM(Org_amt), 2) as total_amount
                FROM tb_acc_txn
            """)
            trans_stats = self.cursor.fetchone()
            if trans_stats:
                total_count, avg_amount, max_amount, min_amount, total_amount = trans_stats
                print(f"  总交易笔数     : {total_count:6d} 笔")
                print(f"  平均交易金额   : {avg_amount:10.2f} 元")
                print(f"  最大交易金额   : {max_amount:10.2f} 元")
                print(f"  最小交易金额   : {min_amount:10.2f} 元")
                print(f"  交易总金额     : {total_amount:12.2f} 元")
        except Exception as e:
            print(f"交易统计失败: {e}")

        print("="*50)

    def run_all(self):
        """执行所有数据生成步骤"""
        start_time = time.time()

        print("=== AML-EDD反洗钱数据库生成开始 ===\n")

        try:
            # 1. 连接数据库
            if not self.connect():
                return False

            # 2. 创建表结构
            if not self.create_tables():
                return False

            # 3. 生成基础数据
            self.generate_bank_data()
            self.generate_settle_type_data()

            # 4. 生成客户数据（演示版本，减少数量）
            self.generate_person_customers(100)  # 100个个人客户
            self.generate_unit_customers(20)     # 20个企业客户

            # 5. 生成账户数据
            self.generate_accounts()

            # 6. 生成交易数据（演示版本，减少数量）
            self.generate_transactions(1000)     # 1000条交易记录

            # 7. 生成风险等级数据
            self.generate_risk_levels()

            # 8. 生成统计报告
            self.generate_statistics_report()

            execution_time = time.time() - start_time
            print(f"\n[SUCCESS] 数据库生成完成！总耗时: {execution_time:.2f} 秒")
            print(f"[FILE] 数据库文件: {self.db_path}")
            if os.path.exists(self.db_path):
                print(f"[SIZE] 数据库大小: {os.path.getsize(self.db_path) / (1024*1024):.2f} MB")

            return True

        except Exception as e:
            print(f"[ERROR] 数据库生成失败: {e}")
            return False

        finally:
            if self.connection:
                self.connection.close()

def main():
    """主函数"""
    import platform

    print("AML-EDD反洗钱数据库SQLite演示程序")
    print(f"运行环境: {platform.python_version()}")
    print(f"操作系统: {platform.system()}")
    print("注意: 这是演示版本，数据量已减少以加快执行速度\n")

    generator = AMLDatabaseGenerator()
    success = generator.run_all()

    if success:
        print("\n[COMPLETE] 成功生成AML-EDD演示数据库！")
        print("\n下一步操作:")
        print("1. 可以使用SQLite客户端查看数据库内容")
        print("2. 可以运行SQL查询进行数据分析")
        print("3. 可以用于AML-EDD系统的开发和测试")
        print("\n要生成完整数据（1000个人客户+100企业客户+10000交易），请使用原版SQL程序")
        return True
    else:
        print("\n[FAIL] 数据库生成失败，请检查错误信息")
        return False

if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)