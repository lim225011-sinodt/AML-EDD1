#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化版大规模AML300数据生成器
按照300号文要求生成：
- 1000个个人客户
- 100个企业客户
- 10000条交易记录
时间维度：
- 开户时间：2010年1月1日 - 2025年1月1日
- 交易检查期：2020年1月1日 - 2020年6月30日（6个月）
"""

import mysql.connector
import random
import time
from datetime import datetime, timedelta
import sys

class SimpleLargeScaleAML300DataGenerator:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.bank_codes = []
        self.settle_types = []

        # 时间范围定义
        self.account_open_start = datetime(2010, 1, 1)
        self.account_open_end = datetime(2025, 1, 1)
        self.txn_check_start = datetime(2020, 1, 1)
        self.txn_check_end = datetime(2020, 6, 30)

        # 风险等级定义：01-高, 02-中高, 03-中, 04-低
        self.risk_levels = {
            '01': {'name': '高风险', 'ratio': 0.05},    # 5%
            '02': {'name': '中高风险', 'ratio': 0.15},  # 15%
            '03': {'name': '中风险', 'ratio': 0.50},    # 50%
            '04': {'name': '低风险', 'ratio': 0.30}     # 30%
        }

    def connect_database(self):
        """连接数据库"""
        try:
            self.conn = mysql.connector.connect(
                host='101.42.102.9',
                port=3306,
                user='root',
                password='Bancstone123!',
                database='AML300',
                charset='utf8mb4'
            )
            self.cursor = self.conn.cursor()
            print("[OK] 数据库连接成功")
            return True
        except Exception as e:
            print(f"[ERROR] 数据库连接失败: {e}")
            return False

    def get_bank_codes(self):
        """获取银行分行代码"""
        try:
            self.cursor.execute("SELECT Bank_code1, Bank_name FROM tb_bank")
            self.bank_codes = self.cursor.fetchall()
            print(f"[OK] 获取到 {len(self.bank_codes)} 个银行分行")
            return len(self.bank_codes) > 0
        except Exception as e:
            print(f"[ERROR] 获取银行代码失败: {e}")
            return False

    def get_settle_types(self):
        """获取业务类型"""
        try:
            self.cursor.execute("SELECT Settle_type, Settle_name FROM tb_settle_type")
            self.settle_types = self.cursor.fetchall()
            print(f"[OK] 获取到 {len(self.settle_types)} 个业务类型")
            return len(self.settle_types) > 0
        except Exception as e:
            print(f"[WARNING] 获取业务类型失败: {e}")
            # 提供默认业务类型
            self.settle_types = [('ST001', '转账'), ('ST002', '存款'), ('ST003', '取款')]
            return True

    def clear_existing_data(self):
        """清理现有数据"""
        print("\n[INFO] 清理现有数据...")
        tables = [
            'tb_lar_report', 'tb_sus_report', 'tb_lwhc_log', 'tb_cross_border',
            'tb_cash_convert', 'tb_cash_remit', 'tb_cred_txn', 'tb_acc_txn',
            'tb_risk_his', 'tb_risk_new', 'tb_acc', 'tb_cst_unit', 'tb_cst_pers'
        ]

        try:
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            for table in tables:
                self.cursor.execute(f"DELETE FROM {table}")
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            self.conn.commit()
            print("[OK] 现有数据清理完成")
            return True
        except Exception as e:
            print(f"[ERROR] 数据清理失败: {e}")
            return False

    def generate_personal_customers(self, count=1000):
        """生成个人客户数据"""
        print(f"\n[INFO] 生成 {count} 个个人客户...")

        sql = """INSERT INTO tb_cst_pers (Head_no, Bank_code1, Cst_no, Open_time, Close_time, Acc_name,
                                         Cst_sex, Nation, Id_type, Id_no, Id_deadline, Occupation, Income,
                                         Contact1, Contact2, Contact3, Address1, Address2, Address3, Company, Sys_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        occupations = ['软件工程师', '数据分析师', '产品经理', '市场营销', '财务经理', '人力资源', '销售经理',
                      '项目经理', '技术总监', '运营经理', '客服专员', '行政助理', '采购专员', '物流经理', '品质管理']

        surnames = ['张', '王', '李', '赵', '刘', '陈', '杨', '黄', '周', '吴', '徐', '孙', '马', '朱', '胡']
        names = ['伟', '芳', '娜', '敏', '静', '丽', '强', '磊', '军', '洋', '勇', '艳', '杰', '娟', '涛']

        batch_data = []

        for i in range(1, count + 1):
            # 随机开户时间（2010-2025年）
            open_date = self.account_open_start + timedelta(
                days=random.randint(0, (self.account_open_end - self.account_open_start).days)
            )
            open_date_str = open_date.strftime('%Y%m%d')

            # 5%的客户已销户
            is_closed = random.random() < 0.05
            close_date = None
            if is_closed:
                close_date = (open_date + timedelta(days=random.randint(365, 3650))).strftime('%Y%m%d')

            # 随机选择分行
            bank_code = random.choice(self.bank_codes)[0]

            # 生成个人基本信息
            person = random.choice(surnames) + random.choice(names)
            gender = random.choice(['11', '12'])  # 11-男, 12-女

            # 生成身份证号码
            birth_year = random.randint(1960, 2000)
            id_no = f"{random.choice(['110', '310', '440', '510'])}" + \
                    f"{str(birth_year).zfill(2)}{str(random.randint(1, 12)).zfill(2)}{str(random.randint(1, 28)).zfill(2)}" + \
                    f"{str(random.randint(1001, 9999))}"

            # 生成联系方式
            phone = f"1{random.choice([3, 4, 5, 6, 7, 8, 9])}{str(random.randint(100000000, 999999999))}"

            # 生成地址
            provinces = ['北京市', '上海市', '广东省', '江苏省', '浙江省', '山东省', '河南省', '四川省']
            cities = ['朝阳区', '海淀区', '西城区', '东城区', '浦东新区', '黄浦区', '天河区', '福田区']
            streets = ['建国路', '人民路', '解放路', '中山路', '和平路', '胜利路', '文化路', '科技路']
            address = f"{random.choice(provinces)}{random.choice(cities)}{random.choice(streets)}{random.randint(1, 999)}号"

            # 收入水平（根据年龄和职业估算）
            age = 2024 - birth_year
            base_income = 80000 + (age * 1000) + random.randint(-20000, 50000)

            data = (
                "ABC001",  # Head_no
                bank_code,  # Bank_code1
                f"P{str(i).zfill(6)}",  # Cst_no
                open_date_str,  # Open_time
                close_date,  # Close_time
                person,  # Acc_name
                gender,  # Cst_sex
                "CHN",  # Nation
                "11",  # Id_type (身份证)
                id_no,  # Id_no
                "20300101",  # Id_deadline
                random.choice(occupations),  # Occupation
                base_income,  # Income
                phone,  # Contact1
                None,  # Contact2
                None,  # Contact3
                address,  # Address1
                None,  # Address2
                None,  # Address3
                f"公司{str(i).zfill(3)}",  # Company
                f"个人系统{str(i).zfill(3)}"  # Sys_name
            )

            batch_data.append(data)

            # 每100条提交一次
            if len(batch_data) >= 100:
                self.cursor.executemany(sql, batch_data)
                self.conn.commit()
                batch_data = []
                print(f"  已生成 {i} 个个人客户")

        # 提交剩余数据
        if batch_data:
            self.cursor.executemany(sql, batch_data)
            self.conn.commit()

        print(f"[OK] 个人客户生成完成：{count} 个")
        return True

    def generate_corporate_customers(self, count=100):
        """生成企业客户数据"""
        print(f"\n[INFO] 生成 {count} 个企业客户...")

        sql = """INSERT INTO tb_cst_unit (Head_no, Bank_code1, Cst_no, Open_time, Acc_name, Rep_name,
                                         Ope_name, License, Id_deadline, Industry, Reg_amt, Reg_amt_code, Sys_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        industries = ['软件开发', '金融服务', '贸易公司', '制造业', '房地产', '建筑工程',
                     '物流运输', '医药制造', '教育培训', '咨询服务', '电子商务', '能源开发']

        batch_data = []

        for i in range(1, count + 1):
            # 随机开户时间
            open_date = self.account_open_start + timedelta(
                days=random.randint(0, (self.account_open_end - self.account_open_start).days)
            )
            open_date_str = open_date.strftime('%Y%m%d')

            # 随机选择分行
            bank_code = random.choice(self.bank_codes)[0]

            # 生成企业信息
            company_prefixes = ['华夏', '国泰', '万达', '恒大', '阿里巴巴', '腾讯', '百度', '华为', '中兴']
            company_suffixes = ['科技有限公司', '贸易有限公司', '投资有限公司', '实业有限公司']
            company_name = f"{random.choice(company_prefixes)}{str(i)}{random.choice(company_suffixes)}"

            # 生成统一社会信用代码
            license_no = f"{str(random.randint(10000000, 99999999))}" + \
                        f"{str(random.randint(10000000, 99999999))}"

            # 注册资本（根据行业设定）
            base_capital = {
                '房地产开发': 50000000,
                '建筑工程': 30000000,
                '金融服务': 100000000,
                '制造业': 20000000,
                '软件开发': 10000000
            }
            industry = random.choice(industries)
            reg_amt = base_capital.get(industry, 10000000) + random.randint(-5000000, 20000000)

            data = (
                "ABC001",  # Head_no
                bank_code,  # Bank_code1
                f"U{str(i).zfill(6)}",  # Cst_no
                open_date_str,  # Open_time
                company_name,  # Acc_name
                f"法人{str(i)}",  # Rep_name (法定代表人)
                f"经办{str(i)}",  # Ope_name (经办人)
                license_no,  # License
                "20300101",  # Id_deadline
                industry,  # Industry
                reg_amt,  # Reg_amt
                "CNY",  # Reg_amt_code
                f"企业系统{str(i).zfill(3)}"  # Sys_name
            )

            batch_data.append(data)

            # 每50条提交一次
            if len(batch_data) >= 50:
                self.cursor.executemany(sql, batch_data)
                self.conn.commit()
                batch_data = []
                print(f"  已生成 {i} 个企业客户")

        # 提交剩余数据
        if batch_data:
            self.cursor.executemany(sql, batch_data)
            self.conn.commit()

        print(f"[OK] 企业客户生成完成：{count} 个")
        return True

    def generate_accounts(self):
        """生成账户数据"""
        print("\n[INFO] 生成账户数据...")

        # 获取客户信息
        self.cursor.execute("SELECT Cst_no, Bank_code1, Id_no, Open_time FROM tb_cst_pers LIMIT 1000")
        personal_customers = self.cursor.fetchall()

        self.cursor.execute("SELECT Cst_no, Bank_code1, License, Open_time FROM tb_cst_unit LIMIT 100")
        corporate_customers = self.cursor.fetchall()

        print(f"  个人客户：{len(personal_customers)} 个")
        print(f"  企业客户：{len(corporate_customers)} 个")

        sql = """INSERT INTO tb_acc (Head_no, Bank_code1, Self_acc_name, Acc_state, Self_acc_no, Card_no,
                                    Acc_type, Acc_type1, Id_no, Cst_no, Open_time, Close_time,
                                    Agency_flag, Acc_flag, Fixed_flag)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        batch_data = []
        account_count = 0

        # 生成个人账户（每人1-3个账户）
        for cst_no, bank_code, id_no, open_time in personal_customers:
            num_accounts = random.randint(1, 3)
            for i in range(num_accounts):
                account_count += 1

                # 生成账户号
                acc_no = f"6228{str(random.randint(1000000000000000, 9999999999999999))}"
                card_no = f"6225{str(random.randint(1000000000000000, 9999999999999999))}"

                # 获取客户姓名
                self.cursor.execute("SELECT Acc_name FROM tb_cst_pers WHERE Cst_no = %s", (cst_no,))
                acc_name = self.cursor.fetchone()[0]

                # 账户类型
                acc_type = random.choice(['11', '12', '14'])  # 储蓄、定期、信用卡

                # 10%的账户已销户
                is_closed = random.random() < 0.10
                close_date = None
                if is_closed:
                    close_date = (datetime.strptime(open_time, '%Y%m%d') +
                                timedelta(days=random.randint(365, 3650))).strftime('%Y%m%d')

                data = (
                    "ABC001", bank_code, acc_name, "11", acc_no, card_no,
                    acc_type, "21", id_no, cst_no, open_time, close_date,
                    None, None, None
                )
                batch_data.append(data)

                if len(batch_data) >= 100:
                    self.cursor.executemany(sql, batch_data)
                    self.conn.commit()
                    batch_data = []
                    print(f"  已生成 {account_count} 个账户")

        # 生成企业账户（每企业1-2个账户）
        for cst_no, bank_code, license_no, open_time in corporate_customers:
            num_accounts = random.randint(1, 2)
            for i in range(num_accounts):
                account_count += 1

                acc_no = f"6229{str(random.randint(1000000000000000, 9999999999999999))}"

                self.cursor.execute("SELECT Acc_name FROM tb_cst_unit WHERE Cst_no = %s", (cst_no,))
                acc_name = self.cursor.fetchone()[0]

                data = (
                    "ABC001", bank_code, acc_name, "11", acc_no, None,
                    "13", "23", license_no, cst_no, open_time, None,
                    None, None, None
                )
                batch_data.append(data)

                if len(batch_data) >= 100:
                    self.cursor.executemany(sql, batch_data)
                    self.conn.commit()
                    batch_data = []
                    print(f"  已生成 {account_count} 个账户")

        # 提交剩余数据
        if batch_data:
            self.cursor.executemany(sql, batch_data)
            self.conn.commit()

        print(f"[OK] 账户生成完成：{account_count} 个")
        return True

    def generate_risk_levels(self):
        """生成风险等级数据"""
        print("\n[INFO] 生成风险等级数据...")

        # 获取所有客户
        self.cursor.execute("SELECT Cst_no, Id_no FROM tb_cst_pers")
        personal_customers = self.cursor.fetchall()

        self.cursor.execute("SELECT Cst_no, License FROM tb_cst_unit")
        corporate_customers = self.cursor.fetchall()

        total_customers = len(personal_customers) + len(corporate_customers)
        print(f"  总客户数：{total_customers}")

        # 生成最新风险等级
        sql_new = """INSERT INTO tb_risk_new (Bank_code1, Cst_no, Self_acc_name, Id_no, Acc_type,
                                             Risk_code, Time, Norm)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""

        # 生成历史风险等级
        sql_his = """INSERT INTO tb_risk_his (Bank_code1, Cst_no, Self_acc_name, Id_no, Acc_type,
                                             Risk_code, Time, Norm)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""

        batch_new = []
        batch_his = []

        # 分配风险等级
        risk_distribution = []
        for code, info in self.risk_levels.items():
            count = int(total_customers * info['ratio'])
            risk_distribution.extend([code] * count)

        # 随机打乱
        random.shuffle(risk_distribution)

        # 为个人客户分配风险等级
        for i, (cst_no, id_no) in enumerate(personal_customers):
            if i < len(risk_distribution):
                risk_code = risk_distribution[i]
                bank_code = random.choice(self.bank_codes)[0]

                self.cursor.execute("SELECT Acc_name FROM tb_cst_pers WHERE Cst_no = %s", (cst_no,))
                acc_name = self.cursor.fetchone()[0]

                # 最新风险等级
                batch_new.append((
                    bank_code, cst_no, acc_name, id_no, "11",
                    risk_code, "20200630", f"客户风险等级评估：{self.risk_levels[risk_code]['name']}"
                ))

                # 历史风险等级（60%的客户有历史记录）
                if random.random() < 0.6:
                    historical_risk = random.choice(['01', '02', '03'])
                    batch_his.append((
                        bank_code, cst_no, acc_name, id_no, "11",
                        historical_risk, "20191231", f"历史风险等级：{self.risk_levels[historical_risk]['name']}"
                    ))

        # 为企业客户分配风险等级
        corp_start_idx = len(personal_customers)
        for i, (cst_no, license_no) in enumerate(corporate_customers):
            idx = corp_start_idx + i
            if idx < len(risk_distribution):
                risk_code = risk_distribution[idx]
                bank_code = random.choice(self.bank_codes)[0]

                self.cursor.execute("SELECT Acc_name FROM tb_cst_unit WHERE Cst_no = %s", (cst_no,))
                acc_name = self.cursor.fetchone()[0]

                # 最新风险等级
                batch_new.append((
                    bank_code, cst_no, acc_name, license_no, "13",
                    risk_code, "20200630", f"企业风险等级评估：{self.risk_levels[risk_code]['name']}"
                ))

                # 历史风险等级（80%的企业有历史记录）
                if random.random() < 0.8:
                    historical_risk = random.choice(['01', '02', '03'])
                    batch_his.append((
                        bank_code, cst_no, acc_name, license_no, "13",
                        historical_risk, "20191231", f"历史风险等级：{self.risk_levels[historical_risk]['name']}"
                    ))

        # 批量插入
        if batch_new:
            self.cursor.executemany(sql_new, batch_new)
            self.conn.commit()
            print(f"[OK] 最新风险等级生成完成：{len(batch_new)} 条")

        if batch_his:
            self.cursor.executemany(sql_his, batch_his)
            self.conn.commit()
            print(f"[OK] 历史风险等级生成完成：{len(batch_his)} 条")

        return True

    def generate_transactions(self, target_count=10000):
        """生成交易数据"""
        print(f"\n[INFO] 生成 {target_count} 条交易记录...")

        # 获取账户信息
        self.cursor.execute("SELECT Self_acc_no, Cst_no, Id_no, Acc_type FROM tb_acc")
        accounts = self.cursor.fetchall()

        if len(accounts) == 0:
            print("[ERROR] 没有找到账户数据")
            return False

        print(f"  可用账户数：{len(accounts)}")

        sql = """INSERT INTO tb_acc_txn (Date, Time, Self_bank_code, Acc_type, Cst_no, Id_no, Self_acc_no,
                                        Card_no, Part_acc_no, Part_acc_name, Lend_flag, Tsf_flag, Reverse_flag,
                                        Cur, Org_amt, Usd_amt, Rmb_amt, Balance, Purpose, Bord_flag, Nation,
                                        Bank_flag, Ip_code, Atm_code, Bank_code, Mac_info, Settle_type, Ticd)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s)"""

        batch_data = []

        for i in range(target_count):
            # 随机交易时间（2020年1月1日 - 2020年6月30日）
            txn_date = self.txn_check_start + timedelta(
                days=random.randint(0, (self.txn_check_end - self.txn_check_start).days)
            )
            date_str = txn_date.strftime('%Y%m%d')
            time_str = f"{str(random.randint(8, 20)).zfill(2)}{str(random.randint(0, 59)).zfill(2)}{str(random.randint(0, 59)).zfill(2)}"

            # 随机选择账户
            acc_no, cst_no, id_no, acc_type = random.choice(accounts)

            # 获取账户信息
            self.cursor.execute("SELECT Self_acc_name, Card_no FROM tb_acc WHERE Self_acc_no = %s", (acc_no,))
            acc_info = self.cursor.fetchone()
            acc_name = acc_info[0] if acc_info else "账户"
            card_no = acc_info[1] if acc_info else None

            # 交易对手信息
            part_names = ['张三', '李四', '王五', '赵六', '钱七', '孙八', '周九', '吴十']
            part_acc_name = random.choice(part_names)
            part_nation = random.choice(['CHN', 'USA', 'GBR', 'HKG', 'JPN', 'KOR', 'SGP'])

            # 交易金额（根据账户类型）
            if acc_type == '13':  # 企业账户
                org_amt = random.uniform(10000, 500000)
            else:  # 个人账户
                org_amt = random.uniform(100, 50000)

            # 汇率转换
            usd_rate = 6.8 + random.uniform(-0.5, 0.5)
            usd_amt = org_amt / usd_rate
            rmb_amt = org_amt  # 原币为人民币

            # 余额
            balance = random.uniform(1000, 1000000)

            # 交易用途
            purposes = ['货款', '工资', '服务费', '投资款', '借款还款', '消费', '转账', '其他']
            purpose = random.choice(purposes)

            # IP地址
            ip_code = f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}"

            # 业务类型
            if self.settle_types:
                settle_type = random.choice(self.settle_types)[0]
            else:
                settle_type = "ST001"

            data = (
                date_str, time_str,
                random.choice(self.bank_codes)[0],  # Self_bank_code
                acc_type,  # Acc_type
                cst_no,  # Cst_no
                id_no,  # Id_no
                acc_no,  # Self_acc_no
                card_no,  # Card_no
                f"6228{str(random.randint(1000000000000000, 9999999999999999))}",  # Part_acc_no
                part_acc_name,  # Part_acc_name
                random.choice(['10', '11']),  # Lend_flag
                random.choice(['10', '11']),  # Tsf_flag
                '10',  # Reverse_flag
                random.choice(['CNY', 'USD']),  # Cur
                round(org_amt, 2),  # Org_amt
                round(usd_amt, 2),  # Usd_amt
                round(rmb_amt, 2),  # Rmb_amt
                round(balance, 2),  # Balance
                purpose,  # Purpose
                random.choice(['11', '12']),  # Bord_flag
                part_nation,  # Nation
                '11',  # Bank_flag
                ip_code,  # Ip_code
                f"ATM{str(random.randint(1, 999)).zfill(3)}",  # Atm_code
                random.choice(self.bank_codes)[0],  # Bank_code
                f"MAC{str(random.randint(100000000, 999999999))}",  # Mac_info
                settle_type,  # Settle_type
                f"TXN{date_str}{str(random.randint(1, 999999)).zfill(6)}"  # Ticd
            )

            batch_data.append(data)

            # 每500条提交一次
            if len(batch_data) >= 500:
                self.cursor.executemany(sql, batch_data)
                self.conn.commit()
                batch_data = []
                print(f"  已生成 {i+1} 条交易记录")

        # 提交剩余数据
        if batch_data:
            self.cursor.executemany(sql, batch_data)
            self.conn.commit()

        print(f"[OK] 交易记录生成完成：{target_count} 条")
        return True

    def generate_cross_border_transactions(self, count=500):
        """生成跨境交易数据"""
        print(f"\n[INFO] 生成 {count} 条跨境交易记录...")

        # 获取部分账户用于跨境交易
        self.cursor.execute("SELECT Self_acc_no, Cst_no, Id_no, Self_acc_name FROM tb_acc LIMIT 200")
        accounts = self.cursor.fetchall()

        sql = """INSERT INTO tb_cross_border (Date, Time, Lend_flag, Tsf_flag, Unit_flag, Cst_no, Id_no,
                                            Self_acc_name, Self_acc_no, Card_no, Self_add, Ticd, Part_acc_no,
                                            Part_acc_name, Part_nation, Cur, Org_amt, Usd_amt, Rmb_amt, Balance,
                                            Agency_flag, Agent_name, Agent_tel, Agent_type, Agent_no, Settle_type,
                                            Reverse_flag, Purpose, Bord_flag, Nation, Bank_flag, Ip_code,
                                            Atm_code, Bank_code, Mac_info)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        batch_data = []

        for i in range(count):
            # 随机交易时间
            txn_date = self.txn_check_start + timedelta(
                days=random.randint(0, (self.txn_check_end - self.txn_check_start).days)
            )
            date_str = txn_date.strftime('%Y%m%d')
            time_str = f"{str(random.randint(8, 20)).zfill(2)}{str(random.randint(0, 59)).zfill(2)}{str(random.randint(0, 59)).zfill(2)}"

            acc_no, cst_no, id_no, acc_name = random.choice(accounts)

            # 跨境交易金额通常较大
            org_amt = random.uniform(50000, 1000000)
            usd_rate = 6.8 + random.uniform(-0.5, 0.5)
            usd_amt = org_amt / usd_rate
            rmb_amt = org_amt

            # 跨境交易对手
            part_nation = random.choice(['USA', 'GBR', 'HKG', 'JPN', 'KOR', 'SGP', 'AUS', 'CAN'])

            data = (
                date_str, time_str,
                random.choice(['10', '11']),  # Lend_flag
                random.choice(['10', '11']),  # Tsf_flag
                random.choice(['10', '11']),  # Unit_flag
                cst_no, id_no, acc_name, acc_no, None,  # 基本账户信息
                f"北京市朝阳区建国路{random.randint(1, 999)}号",  # Self_add
                f"CROSS{date_str}{str(random.randint(1, 999999)).zfill(6)}",  # Ticd
                f"OVERSEAS{str(random.randint(1000000000000000, 9999999999999999))}",  # Part_acc_no
                f"海外公司{str(i)}",  # Part_acc_name
                part_nation,  # Part_nation
                random.choice(['USD', 'EUR', 'GBP', 'JPY', 'HKD']),  # Cur
                round(org_amt, 2), round(usd_amt, 2), round(rmb_amt, 2),
                random.uniform(100000, 5000000),  # Balance
                '11',  # Agency_flag
                f"代理机构{str(i)}",  # Agent_name
                f"400{str(random.randint(10000000, 99999999))}",  # Agent_tel
                random.choice(['01', '02', '03']),  # Agent_type
                f"AGENT{str(random.randint(100000, 999999))}",  # Agent_no
                random.choice(self.settle_types)[0] if self.settle_types else "CB001",  # Settle_type
                '10',  # Reverse_flag
                random.choice(['货款进口', '服务费支付', '投资款', '货款出口', '技术服务']),  # Purpose
                '12',  # Bord_flag
                part_nation,  # Nation
                '11',  # Bank_flag
                f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",  # Ip_code
                f"ATM{str(random.randint(1, 999)).zfill(3)}",  # Atm_code
                random.choice(self.bank_codes)[0],  # Bank_code
                f"MAC{str(random.randint(100000000, 999999999))}"  # Mac_info
            )

            batch_data.append(data)

        if batch_data:
            self.cursor.executemany(sql, batch_data)
            self.conn.commit()

        print(f"[OK] 跨境交易记录生成完成：{count} 条")
        return True

    def generate_other_data(self):
        """生成其他类型数据"""
        print("\n[INFO] 生成其他业务数据...")

        # 生成简化的其他数据
        self.generate_simplified_other_data()
        return True

    def generate_simplified_other_data(self):
        """生成简化的其他数据"""
        print("  生成简化版其他数据...")

        # 信用卡交易（简化版）
        self.cursor.execute("SELECT Self_acc_no, Cst_no, Id_no FROM tb_acc WHERE Acc_type = '14' LIMIT 100")
        credit_accounts = self.cursor.fetchall()
        if not credit_accounts:
            self.cursor.execute("SELECT Self_acc_no, Cst_no, Id_no FROM tb_acc LIMIT 100")
            credit_accounts = self.cursor.fetchall()

        sql_credit = """INSERT INTO tb_cred_txn (Self_acc_no, Card_no, Self_acc_name, Cst_no, Id_no,
                                               Date, Time, Lend_flag, Tsf_flag, Cur, Org_amt, Usd_amt, Rmb_amt,
                                               Balance, Purpose, Pos_owner, Trans_type, Ip_code, Bord_flag, Nation)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        batch_credit = []
        for i in range(1000):
            acc_no, cst_no, id_no = random.choice(credit_accounts)
            txn_date = self.txn_check_start + timedelta(
                days=random.randint(0, (self.txn_check_end - self.txn_check_start).days)
            )

            data = (
                acc_no,
                f"6225{str(random.randint(1000000000000000, 9999999999999999))}",
                f"信用卡账户{acc_no[-4:]}",
                cst_no, id_no,
                txn_date.strftime('%Y%m%d'),
                f"{str(random.randint(8, 22)).zfill(2)}{str(random.randint(0, 59)).zfill(2)}{str(random.randint(0, 59)).zfill(2)}",
                random.choice(['10', '11']), random.choice(['10', '11']),
                'CNY',
                round(random.uniform(50, 20000), 2),
                round(random.uniform(7, 3000), 2),
                round(random.uniform(50, 20000), 2),
                round(random.uniform(1000, 100000), 2),
                random.choice(['消费', '取现', '转账', '缴费']),
                f"商户{str(i)}",
                random.choice(['11', '12', '13']),
                f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",
                '12', 'CHN'
            )
            batch_credit.append(data)

        if batch_credit:
            self.cursor.executemany(sql_credit, batch_credit)
            self.conn.commit()
        print(f"    [OK] 信用卡交易生成完成：{len(batch_credit)} 条")

        # 现金汇款（简化版）
        sql_cash = """INSERT INTO tb_cash_remit (Date, Time, Lend_flag, Tsf_flag, Cst_no, Id_no,
                                             Self_acc_name, Self_acc_no, Cur, Org_amt, Usd_amt, Rmb_amt,
                                             Balance, Part_acc_no, Part_acc_name, Part_nation, Settle_type,
                                             Reverse_flag, Purpose, Bord_flag, Nation, Bank_flag, Ip_code,
                                             Atm_code, Bank_code, Mac_info)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                             %s, %s, %s, %s, %s, %s, %s)"""

        batch_cash = []
        for i in range(500):
            txn_date = self.txn_check_start + timedelta(
                days=random.randint(0, (self.txn_check_end - self.txn_check_start).days)
            )

            data = (
                txn_date.strftime('%Y%m%d'),
                f"{str(random.randint(8, 18)).zfill(2)}{str(random.randint(0, 59)).zfill(2)}{str(random.randint(0, 59)).zfill(2)}",
                random.choice(['10', '11']), random.choice(['10', '11']),
                f"P{str(random.randint(1, 1000)).zfill(6)}",
                f"1101011990{str(random.randint(1, 999)).zfill(3)}0101234",
                f"客户{str(i)}",
                f"6228{str(random.randint(1000000000000000, 9999999999999999))}",
                'CNY',
                round(random.uniform(5000, 50000), 2),
                round(random.uniform(700, 7000), 2),
                round(random.uniform(5000, 50000), 2),
                round(random.uniform(10000, 100000), 2),
                f"6228{str(random.randint(1000000000000000, 9999999999999999))}",
                f"收款人{str(i)}",
                'CHN',
                random.choice(self.settle_types)[0] if self.settle_types else "CR001",
                '10',
                random.choice(['现金汇款', '无卡存款']),
                '11', 'CHN', '11',
                f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",
                f"ATM{str(random.randint(1, 999)).zfill(3)}",
                random.choice(self.bank_codes)[0],
                f"MAC{str(random.randint(100000000, 999999999))}"
            )
            batch_cash.append(data)

        if batch_cash:
            self.cursor.executemany(sql_cash, batch_cash)
            self.conn.commit()
        print(f"    [OK] 现金汇款生成完成：{len(batch_cash)} 条")

        # 大额和可疑报告（简化版）
        self.generate_simplified_reports()

    def generate_simplified_reports(self):
        """生成简化的大额和可疑交易报告"""
        # 大额交易报告
        sql_lar = """INSERT INTO tb_lar_report (RLFC, RPMN, Report_Date, Institution_Name, Report_Amount,
                                              Currency, Transaction_Type, Transaction_Date, Customer_Name,
                                              Customer_ID, Account_No)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        batch_lar = []
        for i in range(300):
            report_date = self.txn_check_start + timedelta(
                days=random.randint(0, (self.txn_check_end - self.txn_check_start).days)
            )

            data = (
                random.choice(['00', '01', '02']),
                f"RPM{str(random.randint(1000000000000000, 9999999999999999))}",
                report_date.strftime('%Y%m%d'),
                "中国农业银行总行营业部",
                round(random.uniform(500000, 5000000), 2),
                'CNY',
                random.choice(['现金存款', '现金取款', '转账', '汇款']),
                report_date.strftime('%Y%m%d'),
                f"客户{str(i)}",
                f"P{str(random.randint(1, 1000)).zfill(6)}",
                f"6228{str(random.randint(1000000000000000, 9999999999999999))}"
            )
            batch_lar.append(data)

        if batch_lar:
            self.cursor.executemany(sql_lar, batch_lar)
            self.conn.commit()
        print(f"    [OK] 大额交易报告生成完成：{len(batch_lar)} 条")

        # 可疑交易报告
        sql_sus = """INSERT INTO tb_sus_report (TICD, Report_Date, Institution_Name, Transaction_Amount,
                                              Currency, Transaction_Type, Suspicious_Reason, Report_Time)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""

        batch_sus = []
        suspicious_reasons = [
            '交易金额与客户身份不符',
            '频繁大额现金交易',
            '短时间内多笔可疑交易',
            '与高风险地区交易',
            '交易模式异常',
            '资金来源不明'
        ]

        for i in range(150):
            report_date = self.txn_check_start + timedelta(
                days=random.randint(0, (self.txn_check_end - self.txn_check_start).days)
            )

            data = (
                f"SUS{str(random.randint(1000000000, 9999999999))}",
                report_date.strftime('%Y%m%d'),
                "中国农业银行总行营业部",
                round(random.uniform(100000, 1000000), 2),
                'CNY',
                random.choice(['转账', '现金交易', '跨境交易']),
                random.choice(suspicious_reasons),
                f"{str(random.randint(16, 20)).zfill(2)}{str(random.randint(0, 59)).zfill(2)}{str(random.randint(0, 59)).zfill(2)}"
            )
            batch_sus.append(data)

        if batch_sus:
            self.cursor.executemany(sql_sus, batch_sus)
            self.conn.commit()
        print(f"    [OK] 可疑交易报告生成完成：{len(batch_sus)} 条")

        # 公民联网核查日志
        sql_log = """INSERT INTO tb_lwhc_log (Id_no, Date, Time, Bank_code1, Result, Operator, Reason)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)"""

        batch_log = []
        for i in range(1500):
            log_date = self.txn_check_start + timedelta(
                days=random.randint(0, (self.txn_check_end - self.txn_check_start).days)
            )

            data = (
                f"1101011990{str(random.randint(1, 999)).zfill(3)}0101234",
                log_date.strftime('%Y%m%d'),
                f"{str(random.randint(9, 17)).zfill(2)}{str(random.randint(0, 59)).zfill(2)}{str(random.randint(0, 59)).zfill(2)}",
                random.choice(self.bank_codes)[0],
                random.choice(['01', '02']),
                f"操作员{str(i)}",
                random.choice(['开户核查', '大额交易核查', '可疑交易核查'])
            )
            batch_log.append(data)

        if batch_log:
            self.cursor.executemany(sql_log, batch_log)
            self.conn.commit()
        print(f"    [OK] 公民联网核查日志生成完成：{len(batch_log)} 条")

    def generate_comprehensive_report(self):
        """生成综合报告"""
        print("\n[INFO] 生成数据完整性报告...")

        # 获取各表记录数
        tables_info = [
            ('tb_cst_pers', '个人客户'),
            ('tb_cst_unit', '企业客户'),
            ('tb_acc', '账户'),
            ('tb_risk_new', '最新风险等级'),
            ('tb_risk_his', '历史风险等级'),
            ('tb_acc_txn', '账户交易'),
            ('tb_cross_border', '跨境交易'),
            ('tb_cred_txn', '信用卡交易'),
            ('tb_cash_remit', '现金汇款'),
            ('tb_lwhc_log', '联网核查日志'),
            ('tb_lar_report', '大额交易报告'),
            ('tb_sus_report', '可疑交易报告'),
            ('tb_bank', '银行机构'),
            ('tb_settle_type', '业务类型')
        ]

        print("\n" + "="*60)
        print("AML300大规模数据生成报告")
        print("="*60)

        total_records = 0

        for table, desc in tables_info:
            try:
                self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = self.cursor.fetchone()[0]
                total_records += count

                status = "[OK]" if count > 0 else "[ERROR]"
                print(f"{status} {desc:12} : {count:>8,} 条记录")

                # 显示目标达成情况
                if table == 'tb_cst_pers':
                    target = 1000
                    ratio = count / target * 100
                    print(f"    目标达成: {count}/{target} ({ratio:.1f}%)")
                elif table == 'tb_cst_unit':
                    target = 100
                    ratio = count / target * 100
                    print(f"    目标达成: {count}/{target} ({ratio:.1f}%)")
                elif table == 'tb_acc_txn':
                    target = 10000
                    ratio = count / target * 100
                    print(f"    目标达成: {count}/{target} ({ratio:.1f}%)")

            except Exception as e:
                print(f"[ERROR] {desc:12} : 查询失败 - {e}")

        print("\n" + "-"*60)
        print(f"[INFO] 总记录数: {total_records:,}")
        print("="*60)

        # 数据质量检查
        print("\n[INFO] 数据质量检查:")

        # 检查客户-账户关联
        try:
            self.cursor.execute("""
                SELECT COUNT(*) FROM tb_acc a
                WHERE a.Cst_no NOT IN (
                    SELECT Cst_no FROM tb_cst_pers
                    UNION SELECT Cst_no FROM tb_cst_unit
                )
            """)
            orphan_accounts = self.cursor.fetchone()[0]
            if orphan_accounts == 0:
                print("[OK] 客户-账户关联正常")
            else:
                print(f"[ERROR] 发现 {orphan_accounts} 个孤立账户")
        except:
            print("[WARNING] 客户-账户关联检查失败")

        # 检查风险-客户关联
        try:
            self.cursor.execute("""
                SELECT COUNT(*) FROM tb_risk_new r
                WHERE r.Cst_no NOT IN (
                    SELECT Cst_no FROM tb_cst_pers
                    UNION SELECT Cst_no FROM tb_cst_unit
                )
            """)
            orphan_risks = self.cursor.fetchone()[0]
            if orphan_risks == 0:
                print("[OK] 风险-客户关联正常")
            else:
                print(f"[ERROR] 发现 {orphan_risks} 个孤立风险记录")
        except:
            print("[WARNING] 风险-客户关联检查失败")

        # 时间维度检查
        try:
            self.cursor.execute("""
                SELECT MIN(Date), MAX(Date) FROM tb_acc_txn
                WHERE Date BETWEEN '20200101' AND '20200630'
            """)
            txn_range = self.cursor.fetchone()
            if txn_range[0] and txn_range[1]:
                print(f"[OK] 交易时间范围: {txn_range[0]} - {txn_range[1]}")
            else:
                print("[ERROR] 交易时间范围异常")
        except:
            print("[WARNING] 交易时间检查失败")

        print("\n[INFO] 数据生成策略总结:")
        print("  * 个人客户: 1000个，开户时间2010-2025年")
        print("  * 企业客户: 100个，开户时间2010-2025年")
        print("  * 账户数量: 根据客户数量动态生成")
        print("  * 交易记录: 10000条，时间范围2020年1-6月")
        print("  * 风险等级: 高5%、中高15%、中50%、低30%")
        print("  * 跨境交易: 500条，支持多币种")
        print("  * 其他数据: 完整覆盖300号文件要求")

        return total_records

    def run_full_generation(self):
        """执行完整的数据生成流程"""
        print("[INFO] 开始大规模AML300数据生成")
        print("="*60)

        start_time = time.time()

        try:
            # 1. 连接数据库
            if not self.connect_database():
                return False

            # 2. 获取基础数据
            if not self.get_bank_codes():
                return False
            if not self.get_settle_types():
                print("[WARNING] 业务类型获取失败，使用默认值")

            # 3. 清理现有数据
            if not self.clear_existing_data():
                return False

            # 4. 生成客户数据
            if not self.generate_personal_customers(1000):
                return False
            if not self.generate_corporate_customers(100):
                return False

            # 5. 生成账户数据
            if not self.generate_accounts():
                return False

            # 6. 生成风险等级
            if not self.generate_risk_levels():
                return False

            # 7. 生成交易数据
            if not self.generate_transactions(10000):
                return False

            # 8. 生成跨境交易
            if not self.generate_cross_border_transactions(500):
                return False

            # 9. 生成其他业务数据
            if not self.generate_other_data():
                return False

            # 10. 生成综合报告
            self.generate_comprehensive_report()

            end_time = time.time()
            elapsed = end_time - start_time

            print(f"\n[SUCCESS] 大规模数据生成完成！")
            print(f"[INFO] 总耗时: {elapsed:.2f} 秒")
            print("="*60)

            return True

        except Exception as e:
            print(f"[ERROR] 数据生成过程中发生错误: {e}")
            if self.conn:
                self.conn.rollback()
            return False

        finally:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()

def main():
    """主函数"""
    import time
    generator = SimpleLargeScaleAML300DataGenerator()
    success = generator.run_full_generation()

    if success:
        print("\n[SUCCESS] 大规模AML300数据生成成功完成！")
        print("请检查数据完整性并进行下一步测试。")
        sys.exit(0)
    else:
        print("\n[ERROR] 数据生成失败，请检查错误信息。")
        sys.exit(1)

if __name__ == "__main__":
    main()