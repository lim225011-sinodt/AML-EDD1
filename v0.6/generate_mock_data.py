#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
为AML300数据库15张表生成完整的模拟数据
"""

import mysql.connector
import random
import string
from datetime import datetime, timedelta
import sys

class MockDataGenerator:
    """模拟数据生成器"""

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
        self.surnames = ['张', '王', '李', '赵', '刘', '陈', '杨', '黄', '周', '吴', '徐', '孙', '马', '胡', '郭', '何', '林', '高']
        self.given_names = ['伟', '芳', '娜', '敏', '静', '丽', '强', '磊', '军', '洋', '勇', '艳', '杰', '涛', '明', '超', '华', '平']

        # 企业类型
        self.company_types = ['科技有限公司', '贸易有限公司', '投资公司', '建设集团', '实业有限公司', '发展公司']
        self.company_suffixes = ['有限公司', '股份有限公司', '集团', '控股公司']

        # 银行名称
        self.bank_names = ['中国农业银行', '中国工商银行', '中国建设银行', '中国银行', '招商银行', '民生银行']

        # 业务类型
        self.business_types = ['转账', '存款', '取款', '汇款', '兑换', '消费', '还款', '充值']

        # 币种
        self.currencies = ['CNY', 'USD', 'EUR', 'JPY', 'GBP', 'HKD']

        print("成功连接到AML300数据库，准备生成模拟数据")

    def generate_id_number(self):
        """生成身份证号码"""
        # 18位身份证：6位地区码 + 8位出生日期 + 3位顺序码 + 1位校验码
        area_code = random.choice(['110101', '310101', '440103', '330102', '510104'])
        birth_date = f"{random.randint(1960, 2000):04d}{random.randint(1, 12):02d}{random.randint(1, 28):02d}"
        sequence = f"{random.randint(1, 999):03d}"
        check_code = random.choice('0123456789X')
        return area_code + birth_date + sequence + check_code

    def generate_phone_number(self):
        """生成手机号码"""
        prefixes = ['130', '131', '132', '133', '134', '135', '136', '137', '138', '139', '150', '151', '152', '153', '155', '156', '157', '158', '159', '180', '181', '182', '183', '184', '185', '186', '187', '188', '189']
        return random.choice(prefixes) + ''.join(random.choices('0123456789', k=8))

    def generate_random_date(self, start_date='2023-01-01', end_date='2025-01-09'):
        """生成随机日期"""
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        delta = end - start
        random_days = random.randint(0, delta.days)
        return (start + timedelta(days=random_days)).strftime('%Y-%m-%d')

    def generate_random_amount(self, min_val=100, max_val=1000000):
        """生成随机金额"""
        return round(random.uniform(min_val, max_val), 2)

    def create_banks(self):
        """创建银行信息"""
        print("创建银行信息表...")
        bank_data = []
        for i, bank_name in enumerate(self.bank_names, 1):
            bank_code = f"{i:02d}"
            bank_code1 = f"BK{bank_code}0001"
            bank_data.append((
                bank_code, bank_code1, bank_name,
                f"北京市朝阳区{i}号", "010-12345678"
            ))

        self.cursor.executemany("""
            INSERT INTO tb_bank (Bank_code, Bank_code1, Bank_name, Bank_address, Bank_phone)
            VALUES (%s, %s, %s, %s, %s)
        """, bank_data)
        print(f"已创建 {len(bank_data)} 条银行记录")

    def create_settle_types(self):
        """创建业务类型"""
        print("创建业务类型表...")
        settle_data = []
        for i, business_type in enumerate(self.business_types, 1):
            code = f"ST{i:02d}"
            settle_data.append((code, business_type, f"{business_type}业务描述"))

        self.cursor.executemany("""
            INSERT INTO tb_settle_type (Settle_type, Settle_name, Settle_desc)
            VALUES (%s, %s, %s)
        """, settle_data)
        print(f"已创建 {len(settle_data)} 条业务类型记录")

    def create_individual_customers(self, count=1000):
        """创建个人客户"""
        print(f"创建个人客户表 ({count}条)...")
        cst_pers_data = []

        for i in range(1, count + 1):
            surname = random.choice(self.surnames)
            given_name = random.choice(self.given_names)
            name = surname + given_name
            id_no = self.generate_id_number()
            phone = self.generate_phone_number()

            cst_pers_data.append((
                f"P{i:06d}", name, id_no,
                random.choice(['男', '女']),
                self.generate_random_date('1970-01-01', '2005-12-31'),
                f"{random.choice(['北京市', '上海市', '广州市', '深圳市'])}{random.randint(1, 99)}号",
                phone, f"{name.lower()}@email.com"
            ))

        self.cursor.executemany("""
            INSERT INTO tb_cst_pers (Cst_no, Cst_name, Id_no, Sex, Birthday, Address, Mobile, Email)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, cst_pers_data)
        print(f"已创建 {len(cst_pers_data)} 条个人客户记录")

    def create_corporate_customers(self, count=100):
        """创建企业客户"""
        print(f"创建企业客户表 ({count}条)...")
        cst_unit_data = []

        for i in range(1, count + 1):
            company_type = random.choice(self.company_types)
            suffix = random.choice(self.company_suffixes)
            name = f"{random.choice(['华', '中', '国', '民', '金', '信', '安', '达'])}{random.choice(['兴', '发', '展', '达', '盛', '通', '汇', '融'])}{company_type}{suffix}"

            # 生成统一社会信用代码
            credit_code = ''.join(random.choices('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=18))

            cst_unit_data.append((
                f"U{i:06d}", name, credit_code,
                random.choice(['国有', '民营', '外资', '合资']),
                f"{random.choice(['北京市', '上海市', '广州市', '深圳市'])}{random.randint(1, 999)}号",
                f"{random.choice(['金融', '贸易', '科技', '制造', '服务'])}"
            ))

        self.cursor.executemany("""
            INSERT INTO tb_cst_unit (Cst_no, Unit_name, Credit_code, Unit_type, Address, Industry)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, cst_unit_data)
        print(f"已创建 {len(cst_unit_data)} 条企业客户记录")

    def create_risk_levels(self):
        """创建风险等级信息"""
        print("创建风险等级表...")

        # 获取所有客户
        self.cursor.execute("SELECT Cst_no FROM tb_cst_pers")
        pers_customers = [row[0] for row in self.cursor.fetchall()]

        self.cursor.execute("SELECT Cst_no FROM tb_cst_unit")
        unit_customers = [row[0] for row in self.cursor.fetchall()]

        all_customers = pers_customers + unit_customers

        # 创建最新风险等级
        risk_new_data = []
        for cst_no in all_customers:
            risk_level = random.choice(['低风险', '中风险', '高风险'])
            score = random.randint(1, 100)
            if score < 30:
                risk_level = '低风险'
            elif score < 80:
                risk_level = '中风险'
            else:
                risk_level = '高风险'

            risk_new_data.append((
                cst_no, risk_level, score,
                self.generate_random_date('2024-01-01', '2024-12-31'),
                random.choice(['系统自动评估', '人工评估', '模型评估'])
            ))

        self.cursor.executemany("""
            INSERT INTO tb_risk_new (Cst_no, Risk_level, Risk_score, Eval_date, Eval_method)
            VALUES (%s, %s, %s, %s, %s)
        """, risk_new_data)
        print(f"已创建 {len(risk_new_data)} 条最新风险等级记录")

        # 创建历史风险等级（每个客户2-5条历史记录）
        risk_his_data = []
        for cst_no in all_customers:
            history_count = random.randint(2, 5)
            for i in range(history_count):
                risk_level = random.choice(['低风险', '中风险', '高风险'])
                score = random.randint(1, 100)
                if score < 30:
                    risk_level = '低风险'
                elif score < 80:
                    risk_level = '中风险'
                else:
                    risk_level = '高风险'

                risk_his_data.append((
                    cst_no, risk_level, score,
                    self.generate_random_date('2022-01-01', '2023-12-31'),
                    f"第{i+1}次评估"
                ))

        self.cursor.executemany("""
            INSERT INTO tb_risk_his (Cst_no, Risk_level, Risk_score, Eval_date, Remark)
            VALUES (%s, %s, %s, %s, %s)
        """, risk_his_data)
        print(f"已创建 {len(risk_his_data)} 条历史风险等级记录")

    def create_account_transactions(self, count=10000):
        """创建账户交易记录"""
        print(f"创建账户交易表 ({count}条)...")

        # 获取账户信息
        self.cursor.execute("SELECT Acc_no, Cst_no, Acc_name FROM tb_acc LIMIT 1000")
        accounts = self.cursor.fetchall()

        if not accounts:
            print("没有找到账户信息，跳过交易记录生成")
            return

        # 获取客户身份证号
        cst_id_map = {}
        self.cursor.execute("SELECT Cst_no, Id_no FROM tb_cst_pers")
        for cst_no, id_no in self.cursor.fetchall():
            cst_id_map[cst_no] = id_no

        acc_txn_data = []
        for i in range(1, count + 1):
            acc_no, cst_no, acc_name = random.choice(accounts)
            id_no = cst_id_map.get(cst_no, f"ID{random.randint(100000, 999999)}")

            # 生成交易日期和时间
            date = self.generate_random_date('2024-01-01', '2024-12-31').replace('-', '')
            time = f"{random.randint(8, 20):02d}{random.randint(0, 59):02d}{random.randint(0, 59):02d}"

            # 交易金额
            org_amt = self.generate_random_amount(100, 1000000)
            usd_amt = round(org_amt * random.uniform(0.13, 0.15), 2)  # 假设汇率
            rmb_amt = org_amt

            acc_txn_data.append((
                acc_no, f"CARD{random.randint(1000000000000000, 9999999999999999)}",
                acc_name, cst_no, id_no,
                date, time,
                random.choice(['10', '11']),  # 收付标识
                random.choice(['10', '11']),  # 现金/转账
                random.choice(self.currencies),
                org_amt, usd_amt, rmb_amt,
                random.choice(['日常消费', '转账汇款', '工资收入', '还款', '充值']),
                self.generate_random_amount(0, 50000)  # 余额
            ))

        self.cursor.executemany("""
            INSERT INTO tb_acc_txn (Self_acc_no, Card_no, Self_acc_name, Cst_no, Id_no,
                                   Date, Time, Lend_flag, Tsf_flag, Cur, Org_amt, Usd_amt, Rmb_amt,
                                   Purpose, Balance)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, acc_txn_data)
        print(f"已创建 {len(acc_txn_data)} 条账户交易记录")

    def create_credit_transactions(self, count=2000):
        """创建信用卡交易记录"""
        print(f"创建信用卡交易表 ({count}条)...")

        # 获取客户信息
        self.cursor.execute("SELECT Cst_no, Cst_name, Id_no FROM tb_cst_pers LIMIT 500")
        customers = self.cursor.fetchall()

        if not customers:
            print("没有找到客户信息，跳过信用卡交易生成")
            return

        cred_txn_data = []
        for i in range(1, count + 1):
            cst_no, cst_name, id_no = random.choice(customers)
            acc_no = f"6225{random.randint(1000000000000000, 9999999999999999)}"
            card_no = f"6225{random.randint(1000000000000000, 9999999999999999)}"

            date = self.generate_random_date('2024-01-01', '2024-12-31').replace('-', '')
            time = f"{random.randint(8, 22):02d}{random.randint(0, 59):02d}{random.randint(0, 59):02d}"

            org_amt = self.generate_random_amount(100, 100000)
            usd_amt = round(org_amt * random.uniform(0.13, 0.15), 2)
            rmb_amt = org_amt

            cred_txn_data.append((
                acc_no, card_no, cst_name, cst_no, id_no,
                date, time,
                random.choice(['10', '11']),  # 收付
                random.choice(['10', '11']),  # 现金/转账
                random.choice(['CNY', 'USD']),
                org_amt, usd_amt, rmb_amt,
                self.generate_random_amount(-10000, 5000),  # 溢缴款余额
                random.choice(['POS消费', '网银支付', '取现', '还款', '转账']),
                random.choice(['沃尔玛超市', '天猫商城', '京东购物', '餐饮消费', '加油卡充值']),
                random.choice(['11', '12', '13']),  # 交易类型
                random.choice(['11', '12'])  # 跨境标识
            ))

        self.cursor.executemany("""
            INSERT INTO tb_cred_txn (Self_acc_no, Card_no, Self_acc_name, Cst_no, Id_no,
                                    Date, Time, Lend_flag, Tsf_flag, Cur, Org_amt, Usd_amt, Rmb_amt,
                                    Balance, Purpose, Pos_owner, Trans_type, Bord_flag)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, cred_txn_data)
        print(f"已创建 {len(cred_txn_data)} 条信用卡交易记录")

    def create_cross_border_transactions(self, count=1500):
        """创建跨境交易记录"""
        print(f"创建跨境交易表 ({count}条)...")

        # 获取银行代码
        self.cursor.execute("SELECT Bank_code1 FROM tb_bank")
        banks = [row[0] for row in self.cursor.fetchall()]

        # 获取客户信息
        self.cursor.execute("SELECT Cst_no, Cst_name, Id_no FROM tb_cst_pers LIMIT 300")
        customers = self.cursor.fetchall()

        if not banks or not customers:
            print("缺少银行或客户信息，跳过跨境交易生成")
            return

        # 国家/地区列表
        countries = ['USA', 'GBR', 'JPN', 'HKG', 'SGP', 'AUS', 'CAN', 'DEU', 'FRA', 'KOR']

        cross_border_data = []
        for i in range(1, count + 1):
            cst_no, cst_name, id_no = random.choice(customers)

            date = self.generate_random_date('2024-01-01', '2024-12-31').replace('-', '')
            time = f"{random.randint(8, 18):02d}{random.randint(0, 59):02d}{random.randint(0, 59):02d}"

            acc_no = f"6228{random.randint(1000000000000000, 9999999999999999)}"

            org_amt = self.generate_random_amount(1000, 500000)
            usd_amt = round(org_amt * random.uniform(0.13, 0.15), 2)
            rmb_amt = org_amt

            cross_border_data.append((
                date, time,
                random.choice(['10', '11']),  # 收付标识
                random.choice(['10', '11']),  # 现金/转账
                random.choice(['11', '12']),  # 公私标识
                cst_no, id_no, cst_name, acc_no,
                f"6225{random.randint(1000000000000000, 9999999999999999)}",  # 卡号
                f"北京市朝阳区{i}号",
                f"TX{date}{random.randint(1000, 9999)}",  # 业务流水号
                f"ACC{random.randint(1000000000, 9999999999)}",  # 交易对方账号
                f"Foreign Company {random.randint(1, 999)}",  # 交易对方名称
                random.choice(countries),  # 交易对手国家
                random.choice(['CNY', 'USD']),
                org_amt, usd_amt, rmb_amt,
                self.generate_random_amount(0, 100000),  # 余额
                random.choice(['11', '12']),  # 是否代理
                None, None, None, None,  # 代理信息
                f"ST{random.randint(1, 8):02d}",  # 业务类型
                '10',  # 冲账标识
                random.choice(['货物贸易', '服务贸易', '投资收益', '个人汇款']),
                '11',  # 跨境标识
                random.choice(countries),  # 对方所在国家
                random.choice(['11', '12', '13', '14', '15']),  # 交易方式
                f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 255)}",  # IP
                f"ATM{random.randint(1000, 9999)}",  # ATM编号
                random.choice(banks),  # 机具所属行号
                f"IMEI{random.randint(1000000000000000, 9999999999999999)}"  # MAC/IMEI
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

    def create_accounts(self, count=800):
        """创建账户信息"""
        print(f"创建账户表 ({count}条)...")
        acc_data = []

        # 获取客户编号
        self.cursor.execute("SELECT Cst_no, Cst_name FROM tb_cst_pers LIMIT 600")
        pers_customers = self.cursor.fetchall()

        self.cursor.execute("SELECT Cst_no, Unit_name FROM tb_cst_unit LIMIT 200")
        unit_customers = self.cursor.fetchall()

        all_customers = pers_customers + unit_customers

        for i in range(1, count + 1):
            if i <= len(all_customers):
                cst_no, cst_name = all_customers[i-1]
            else:
                cst_no, cst_name = all_customers[0]

            # 生成账号
            acc_no = f"6228{random.randint(1000000000, 9999999999)}"

            acc_data.append((
                acc_no, cst_no, cst_name,
                random.choice(['活期存款', '定期存款', '借记卡', '信用卡', '公司账户']),
                random.choice(['正常', '冻结', '销户']),
                self.generate_random_date('2020-01-01', '2024-12-31'),
                self.generate_random_amount(0, 100000)
            ))

        self.cursor.executemany("""
            INSERT INTO tb_acc (Acc_no, Cst_no, Acc_name, Acc_type, Acc_status, Open_date, Balance)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, acc_data)
        print(f"已创建 {len(acc_data)} 条账户记录")

    def create_cash_transactions(self):
        """创建现金交易记录"""
        print("创建现金交易记录...")

        # 获取银行代码
        self.cursor.execute("SELECT Bank_code1, Bank_name FROM tb_bank")
        banks = self.cursor.fetchall()

        # 获取客户信息
        self.cursor.execute("SELECT Cst_name, Id_no FROM tb_cst_pers LIMIT 200")
        customers = self.cursor.fetchall()

        # 现金汇款
        cash_remit_data = []
        for i in range(500):
            bank_code, bank_name = random.choice(banks)
            cst_name, id_no = random.choice(customers)

            date = self.generate_random_date('2024-01-01', '2024-12-31').replace('-', '')
            time = f"{random.randint(8, 17):02d}{random.randint(0, 59):02d}{random.randint(0, 59):02d}"

            org_amt = self.generate_random_amount(1000, 50000)
            usd_amt = round(org_amt * random.uniform(0.13, 0.15), 2)
            rmb_amt = org_amt

            cash_remit_data.append((
                date, time, bank_code, cst_name, id_no,
                random.choice(['CNY', 'USD']),
                org_amt, usd_amt, rmb_amt,
                random.choice(['工商银行', '建设银行', '招商银行']),
                f"6228{random.randint(1000000000000000, 9999999999999999)}",
                f"收款人{random.randint(1, 999)}",
                f"ST{random.randint(1, 8):02d}",
                f"CR{date}{random.randint(1000, 9999)}"
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
        for i in range(300):
            bank_code, bank_name = random.choice(banks)
            cst_name, id_no = random.choice(customers)

            date = self.generate_random_date('2024-01-01', '2024-12-31').replace('-', '')
            time = f"{random.randint(8, 17):02d}{random.randint(0, 59):02d}{random.randint(0, 59):02d}"

            out_cur, in_cur = random.sample(['CNY', 'USD', 'EUR', 'JPY'], 2)
            out_amt = self.generate_random_amount(100, 10000)
            out_usd = round(out_amt * random.uniform(0.13, 0.15), 2) if out_cur != 'USD' else out_amt
            in_amt = self.generate_random_amount(100, 10000)
            in_usd = round(in_amt * random.uniform(0.13, 0.15), 2) if in_cur != 'USD' else in_amt

            cash_convert_data.append((
                date, time, bank_code, cst_name, id_no,
                out_cur, out_amt, out_usd,
                in_cur, in_amt, in_usd,
                f"CC{date}{random.randint(1000, 9999)}",
                f"CT{random.randint(1000, 9999)}",
                f"ST{random.randint(1, 8):02d}"
            ))

        self.cursor.executemany("""
            INSERT INTO tb_cash_convert (Date, Time, Self_bank_code, Acc_name, Id_no,
                                        Out_cur, Out_org_amt, Out_usd_amt,
                                        In_cur, In_org_amt, In_usd_amt,
                                        Ticd, Counter_no, Settle_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, cash_convert_data)
        print(f"已创建 {len(cash_convert_data)} 条现钞结售汇记录")

    def create_lwhc_logs(self):
        """创建公民联网核查日志"""
        print("创建公民联网核查日志...")

        # 获取银行信息
        self.cursor.execute("SELECT Bank_name, Bank_code1 FROM tb_bank")
        banks = self.cursor.fetchall()

        # 获取客户信息
        self.cursor.execute("SELECT Cst_name, Id_no FROM tb_cst_pers LIMIT 300")
        customers = self.cursor.fetchall()

        lwhc_log_data = []
        for i in range(800):
            bank_name, bank_code = random.choice(banks)
            cst_name, id_no = random.choice(customers)

            date = self.generate_random_date('2024-01-01', '2024-12-31').replace('-', '')
            time = f"{random.randint(8, 18):02d}{random.randint(0, 59):02d}{random.randint(0, 59):02d}"

            # 核查结果：11-16
            result = random.choice(['11', '12', '13', '14', '15', '16'])

            lwhc_log_data.append((
                bank_name, bank_code, date, time, cst_name, id_no, result,
                f"CT{random.randint(1000, 9999)}",
                random.choice(['个人金融', '公司业务', '信用卡业务', '理财业务']),
                random.choice(['10', '11']),  # 核查方式
                random.choice(['开户核查', '变更核查', '业务办理核查'])
            ))

        self.cursor.executemany("""
            INSERT INTO tb_lwhc_log (Bank_name, Bank_code2, Date, Time, Name, Id_no,
                                     Result, Counter_no, Ope_line, Mode, Purpose)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, lwhc_log_data)
        print(f"已创建 {len(lwhc_log_data)} 条联网核查日志记录")

    def create_reports(self):
        """创建大额和可疑交易报告"""
        print("创建大额和可疑交易报告...")

        # 大额交易报告
        lar_report_data = []
        for i in range(100):
            report_date = self.generate_random_date('2024-01-01', '2024-12-31').replace('-', '')
            trans_date = self.generate_random_date('2024-01-01', '2024-12-31').replace('-', '')

            lar_report_data.append((
                random.choice(['00', '01', '02']),  # RLFC
                None,  # ROTF
                f"RPM{random.randint(1000000000000000, 9999999999999999)}",  # RPMN
                None,  # RPMT
                report_date,
                random.choice(self.bank_names),
                self.generate_random_amount(500000, 50000000),  # 大额
                random.choice(['CNY', 'USD']),
                random.choice(['现金存款', '现金取款', '转账', '跨境汇款']),
                trans_date,
                f"客户{i+1}",
                f"ID{random.randint(100000, 999999)}",
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

        for i in range(50):
            report_date = self.generate_random_date('2024-01-01', '2024-12-31').replace('-', '')
            report_time = f"{random.randint(8, 18):02d}{random.randint(0, 59):02d}"

            sus_report_data.append((
                None, None, None, None, None, None, None, None, None,  # 交易代办人信息（可为空）
                f"TX{report_date}{random.randint(1000, 9999)}",  # TICD
                'CHN000000',  # TRCD
                report_date,
                random.choice(self.bank_names),
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

    def generate_all_data(self):
        """生成所有模拟数据"""
        try:
            print("=== 开始生成AML300数据库模拟数据 ===\n")

            # 清空现有数据（如果需要）
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

            tables = ['tb_lar_report', 'tb_sus_report', 'tb_lwhc_log', 'tb_cross_border',
                     'tb_cash_convert', 'tb_cash_remit', 'tb_cred_txn', 'tb_acc_txn',
                     'tb_risk_new', 'tb_risk_his', 'tb_acc', 'tb_cst_unit', 'tb_cst_pers',
                     'tb_settle_type', 'tb_bank']

            for table in tables:
                self.cursor.execute(f"DELETE FROM {table}")

            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            print("已清空现有数据")

            # 按依赖关系顺序创建数据
            self.create_banks()
            self.create_settle_types()
            self.create_individual_customers(1000)
            self.create_corporate_customers(100)
            self.create_accounts(800)
            self.create_risk_levels()
            self.create_account_transactions(10000)
            self.create_credit_transactions(2000)
            self.create_cross_border_transactions(1500)
            self.create_cash_transactions()
            self.create_lwhc_logs()
            self.create_reports()

            # 提交事务
            self.conn.commit()
            print(f"\n[SUCCESS] 模拟数据生成完成！")

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

            return True

        except Exception as e:
            print(f"[ERROR] 生成模拟数据失败: {e}")
            self.conn.rollback()
            return False

    def close(self):
        """关闭数据库连接"""
        self.cursor.close()
        self.conn.close()

if __name__ == "__main__":
    generator = MockDataGenerator()
    success = generator.generate_all_data()
    generator.close()
    sys.exit(0 if success else 1)