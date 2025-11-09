#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修复版交易数据生成器
严格按照实际表结构生成交易数据
"""

import mysql.connector
import random
import time
from datetime import datetime, timedelta
import sys

class FixedTransactionGenerator:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.bank_codes = []
        self.settle_types = []

        # 时间范围定义
        self.txn_check_start = datetime(2020, 1, 1)
        self.txn_check_end = datetime(2020, 6, 30)

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
            self.cursor.execute("SELECT Settle_type, Name FROM tb_settle_type")
            self.settle_types = self.cursor.fetchall()
            print(f"[OK] 获取到 {len(self.settle_types)} 个业务类型")
            return len(self.settle_types) > 0
        except Exception as e:
            print(f"[WARNING] 获取业务类型失败: {e}")
            # 提供默认业务类型
            self.settle_types = [('ST001', '转账'), ('ST002', '存款'), ('ST003', '取款')]
            return True

    def get_accounts(self):
        """获取账户信息"""
        try:
            self.cursor.execute("SELECT Self_acc_no, Cst_no, Id_no, Acc_type FROM tb_acc")
            accounts = self.cursor.fetchall()
            print(f"[OK] 获取到 {len(accounts)} 个账户")
            return accounts
        except Exception as e:
            print(f"[ERROR] 获取账户信息失败: {e}")
            return []

    def generate_acc_transactions(self, accounts, target_count=10000):
        """生成账户交易数据"""
        print(f"\n[INFO] 生成 {target_count} 条账户交易记录...")

        if len(accounts) == 0:
            print("[ERROR] 没有找到账户数据")
            return False

        sql = """INSERT INTO tb_acc_txn (Date, Time, Self_bank_code, Acc_type, Cst_no, Id_no, Self_acc_no,
                                        Card_no, Part_acc_no, Part_acc_name, Lend_flag, Tsf_flag, Reverse_flag,
                                        Cur, Org_amt, Usd_amt, Rmb_amt, Balance, Agency_flag, Purpose,
                                        Bord_flag, Nation, Bank_flag, Ip_code, Atm_code, Bank_code, Mac_info,
                                        Settle_type, Ticd)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        batch_data = []
        success_count = 0

        for i in range(target_count):
            try:
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
                    random.choice(['11', '12']),  # Agency_flag
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
                success_count += 1

                # 每500条提交一次
                if len(batch_data) >= 500:
                    self.cursor.executemany(sql, batch_data)
                    self.conn.commit()
                    batch_data = []
                    print(f"  已生成 {success_count} 条交易记录")

            except Exception as e:
                print(f"[WARNING] 生成第{i}条交易记录失败: {e}")
                continue

        # 提交剩余数据
        if batch_data:
            try:
                self.cursor.executemany(sql, batch_data)
                self.conn.commit()
            except Exception as e:
                print(f"[ERROR] 批量插入交易记录失败: {e}")
                return False

        print(f"[OK] 交易记录生成完成：{success_count} 条")
        return True

    def generate_cross_border_transactions(self, accounts, count=500):
        """生成跨境交易数据"""
        print(f"\n[INFO] 生成 {count} 条跨境交易记录...")

        if len(accounts) == 0:
            print("[ERROR] 没有找到账户数据")
            return False

        # 获取部分账户用于跨境交易
        accounts_subset = accounts[:200] if len(accounts) > 200 else accounts

        sql = """INSERT INTO tb_cross_border (Date, Time, Lend_flag, Tsf_flag, Unit_flag, Cst_no, Id_no,
                                            Self_acc_name, Self_acc_no, Card_no, Self_add, Ticd, Part_acc_no,
                                            Part_acc_name, Part_nation, Cur, Org_amt, Usd_amt, Rmb_amt, Balance,
                                            Agency_flag, Agent_name, Agent_tel, Agent_type, Agent_no, Settle_type,
                                            Reverse_flag, Purpose, Bord_flag, Nation, Bank_flag, Ip_code,
                                            Atm_code, Bank_code, Mac_info)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        batch_data = []
        success_count = 0

        for i in range(count):
            try:
                # 随机交易时间
                txn_date = self.txn_check_start + timedelta(
                    days=random.randint(0, (self.txn_check_end - self.txn_check_start).days)
                )
                date_str = txn_date.strftime('%Y%m%d')
                time_str = f"{str(random.randint(8, 20)).zfill(2)}{str(random.randint(0, 59)).zfill(2)}{str(random.randint(0, 59)).zfill(2)}"

                acc_no, cst_no, id_no, _ = random.choice(accounts_subset)

                # 获取客户姓名
                self.cursor.execute("""
                    SELECT Acc_name FROM tb_cst_pers WHERE Cst_no = %s
                    UNION
                    SELECT Acc_name FROM tb_cst_unit WHERE Cst_no = %s
                """, (cst_no, cst_no))
                acc_name_result = self.cursor.fetchone()
                acc_name = acc_name_result[0] if acc_name_result else "账户"

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
                success_count += 1

            except Exception as e:
                print(f"[WARNING] 生成第{i}条跨境交易记录失败: {e}")
                continue

        if batch_data:
            try:
                self.cursor.executemany(sql, batch_data)
                self.conn.commit()
            except Exception as e:
                print(f"[ERROR] 批量插入跨境交易记录失败: {e}")
                return False

        print(f"[OK] 跨境交易记录生成完成：{success_count} 条")
        return True

    def generate_other_transactions(self, accounts):
        """生成其他类型的交易数据"""
        print(f"\n[INFO] 生成其他业务数据...")

        # 信用卡交易
        self.generate_credit_transactions(accounts, 1000)

        # 现金汇款
        self.generate_cash_remittances(500)

        # 现钞兑换
        self.generate_cash_conversions(200)

        # 公民联网核查日志
        self.generate_lwhc_logs(1500)

        # 大额和可疑交易报告
        self.generate_reports()

        return True

    def generate_credit_transactions(self, accounts, count=1000):
        """生成信用卡交易"""
        print(f"  生成 {count} 条信用卡交易...")

        # 获取信用卡相关账户
        credit_accounts = [acc for acc in accounts if acc[3] == '14']  # Acc_type = '14' 是信用卡
        if len(credit_accounts) == 0:
            credit_accounts = accounts[:100]  # 如果没有信用卡，使用前100个账户

        sql = """INSERT INTO tb_cred_txn (Self_acc_no, Card_no, Self_acc_name, Cst_no, Id_no,
                                        Date, Time, Lend_flag, Tsf_flag, Cur, Org_amt, Usd_amt, Rmb_amt,
                                        Balance, Purpose, Pos_owner, Trans_type, Ip_code, Bord_flag, Nation)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        batch_data = []
        success_count = 0

        for i in range(count):
            try:
                acc_no, cst_no, id_no, _ = random.choice(credit_accounts)

                txn_date = self.txn_check_start + timedelta(
                    days=random.randint(0, (self.txn_check_end - self.txn_check_start).days)
                )

                # 获取客户姓名
                self.cursor.execute("""
                    SELECT Acc_name FROM tb_cst_pers WHERE Cst_no = %s
                    UNION
                    SELECT Acc_name FROM tb_cst_unit WHERE Cst_no = %s
                """, (cst_no, cst_no))
                acc_name_result = self.cursor.fetchone()
                acc_name = acc_name_result[0] if acc_name_result else "信用卡账户"

                merchants = ['沃尔玛', '天猫超市', '京东商城', '星巴克', '麦当劳', '加油站', '超市', '餐厅', '电影院', '服装店']

                data = (
                    acc_no,
                    f"6225{str(random.randint(1000000000000000, 9999999999999999))}",
                    acc_name,
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
                    random.choice(merchants),
                    random.choice(['11', '12', '13']),
                    f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",
                    '12',
                    random.choice(['CHN', 'USA', 'JPN'])
                )
                batch_data.append(data)
                success_count += 1

            except Exception as e:
                print(f"[WARNING] 生成第{i}条信用卡交易记录失败: {e}")
                continue

        if batch_data:
            try:
                self.cursor.executemany(sql, batch_data)
                self.conn.commit()
            except Exception as e:
                print(f"[ERROR] 批量插入信用卡交易记录失败: {e}")
                return False

        print(f"    [OK] 信用卡交易生成完成：{success_count} 条")
        return True

    def generate_cash_remittances(self, count=500):
        """生成现金汇款"""
        print(f"  生成 {count} 条现金汇款...")

        sql = """INSERT INTO tb_cash_remit (Date, Time, Self_bank_code, Acc_name, Id_no,
                                         Cur, Org_amt, Usd_amt, Rmb_amt, Part_bank, Part_acc_no,
                                         Part_acc_name, Settle_type, Ticd)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        batch_data = []
        success_count = 0

        for i in range(count):
            try:
                txn_date = self.txn_check_start + timedelta(
                    days=random.randint(0, (self.txn_check_end - self.txn_check_start).days)
                )

                part_names = ['张三', '李四', '王五', '赵六', '钱七', '孙八']

                data = (
                    txn_date.strftime('%Y%m%d'),
                    f"{str(random.randint(8, 18)).zfill(2)}{str(random.randint(0, 59)).zfill(2)}{str(random.randint(0, 59)).zfill(2)}",
                    random.choice(self.bank_codes)[0],
                    f"客户{str(i)}",
                    f"1101011990{str(random.randint(1, 999)).zfill(3)}0101234",
                    'CNY',
                    round(random.uniform(5000, 50000), 2),
                    round(random.uniform(700, 7000), 2),
                    round(random.uniform(5000, 50000), 2),
                    random.choice(self.bank_codes)[0],
                    f"6228{str(random.randint(1000000000000000, 9999999999999999))}",
                    random.choice(part_names),
                    random.choice(self.settle_types)[0] if self.settle_types else "CR001",
                    f"CASH{txn_date.strftime('%Y%m%d')}{str(random.randint(1, 999999)).zfill(6)}"
                )
                batch_data.append(data)
                success_count += 1

            except Exception as e:
                print(f"[WARNING] 生成第{i}条现金汇款记录失败: {e}")
                continue

        if batch_data:
            try:
                self.cursor.executemany(sql, batch_data)
                self.conn.commit()
            except Exception as e:
                print(f"[ERROR] 批量插入现金汇款记录失败: {e}")
                return False

        print(f"    [OK] 现金汇款生成完成：{success_count} 条")
        return True

    def generate_cash_conversions(self, count=200):
        """生成现钞兑换"""
        print(f"  生成 {count} 条现钞兑换...")

        sql = """INSERT INTO tb_cash_convert (Date, Time, Self_bank_code, Acc_name, Id_no,
                                           Out_cur, Out_org_amt, Out_usd_amt, In_cur, In_org_amt,
                                           In_usd_amt, Ticd, Counter_no, Settle_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        batch_data = []
        success_count = 0

        for i in range(count):
            try:
                txn_date = self.txn_check_start + timedelta(
                    days=random.randint(0, (self.txn_check_end - self.txn_check_start).days)
                )

                # 外币兑换人民币
                currencies = ['USD', 'EUR', 'GBP', 'JPY', 'HKD', 'KRW']
                out_cur = random.choice(currencies)

                # 汇率
                rates = {'USD': 6.8, 'EUR': 7.5, 'GBP': 8.5, 'JPY': 0.05, 'HKD': 0.85, 'KRW': 0.005}
                rate = rates[out_cur] + random.uniform(-0.1, 0.1)

                out_org_amt = random.uniform(100, 10000)
                in_org_amt = out_org_amt * rate
                out_usd_amt = out_org_amt if out_cur == 'USD' else out_org_amt / rate
                in_usd_amt = in_org_amt / 6.8  # 假设人民币兑美元汇率

                data = (
                    txn_date.strftime('%Y%m%d'),
                    f"{str(random.randint(9, 17)).zfill(2)}{str(random.randint(0, 59)).zfill(2)}{str(random.randint(0, 59)).zfill(2)}",
                    random.choice(self.bank_codes)[0],
                    f"客户{str(i)}",
                    f"1101011990{str(random.randint(1, 999)).zfill(3)}0101234",
                    out_cur, round(out_org_amt, 2), round(out_usd_amt, 2),
                    'CNY', round(in_org_amt, 2), round(in_usd_amt, 2),
                    f"CONV{txn_date.strftime('%Y%m%d')}{str(random.randint(1, 999999)).zfill(6)}",
                    f"COUNTER{str(random.randint(1, 99)).zfill(2)}",
                    random.choice(self.settle_types)[0] if self.settle_types else "CC001"
                )
                batch_data.append(data)
                success_count += 1

            except Exception as e:
                print(f"[WARNING] 生成第{i}条现钞兑换记录失败: {e}")
                continue

        if batch_data:
            try:
                self.cursor.executemany(sql, batch_data)
                self.conn.commit()
            except Exception as e:
                print(f"[ERROR] 批量插入现钞兑换记录失败: {e}")
                return False

        print(f"    [OK] 现钞兑换生成完成：{success_count} 条")
        return True

    def generate_lwhc_logs(self, count=1500):
        """生成公民联网核查日志"""
        print(f"  生成 {count} 条公民联网核查日志...")

        sql = """INSERT INTO tb_lwhc_log (Bank_name, Bank_code2, Date, Time, Name, Id_no, Result,
                                         Counter_no, Ope_line, Mode, Purpose)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        batch_data = []
        success_count = 0

        for i in range(count):
            try:
                log_date = self.txn_check_start + timedelta(
                    days=random.randint(0, (self.txn_check_end - self.txn_check_start).days)
                )
                bank_name, bank_code = random.choice(self.bank_codes)

                data = (
                    bank_name,  # Bank_name
                    bank_code,  # Bank_code2
                    log_date.strftime('%Y%m%d'),
                    f"{str(random.randint(9, 17)).zfill(2)}{str(random.randint(0, 59)).zfill(2)}{str(random.randint(0, 59)).zfill(2)}",
                    f"操作员{str(i)}",  # Name
                    f"1101011990{str(random.randint(1, 999)).zfill(3)}0101234",  # Id_no
                    random.choice(['01', '02']),  # Result
                    f"COUNTER{str(random.randint(1, 99)).zfill(2)}",  # Counter_no
                    f"网点{str(i)}",  # Ope_line
                    random.choice(['01', '02']),  # Mode
                    random.choice(['开户核查', '大额交易核查', '可疑交易核查'])  # Purpose
                )
                batch_data.append(data)
                success_count += 1

            except Exception as e:
                print(f"[WARNING] 生成第{i}条联网核查日志失败: {e}")
                continue

        if batch_data:
            try:
                self.cursor.executemany(sql, batch_data)
                self.conn.commit()
            except Exception as e:
                print(f"[ERROR] 批量插入联网核查日志失败: {e}")
                return False

        print(f"    [OK] 公民联网核查日志生成完成：{success_count} 条")
        return True

    def generate_reports(self):
        """生成大额和可疑交易报告"""
        # 大额交易报告
        self.generate_large_amount_reports(300)

        # 可疑交易报告
        self.generate_suspicious_reports(150)

        return True

    def generate_large_amount_reports(self, count=300):
        """生成大额交易报告"""
        print(f"  生成 {count} 条大额交易报告...")

        sql = """INSERT INTO tb_lar_report (RLFC, RPMN, Report_Date, Institution_Name, Report_Amount,
                                              Currency, Transaction_Type, Transaction_Date, Customer_Name,
                                              Customer_ID, Account_No)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        batch_data = []
        success_count = 0

        for i in range(count):
            try:
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
                batch_data.append(data)
                success_count += 1

            except Exception as e:
                print(f"[WARNING] 生成第{i}条大额交易报告失败: {e}")
                continue

        if batch_data:
            try:
                self.cursor.executemany(sql, batch_data)
                self.conn.commit()
            except Exception as e:
                print(f"[ERROR] 批量插入大额交易报告失败: {e}")
                return False

        print(f"    [OK] 大额交易报告生成完成：{success_count} 条")
        return True

    def generate_suspicious_reports(self, count=150):
        """生成可疑交易报告"""
        print(f"  生成 {count} 条可疑交易报告...")

        sql = """INSERT INTO tb_sus_report (TICD, Report_Date, Institution_Name, Transaction_Amount,
                                              Currency, Transaction_Type, Suspicious_Reason, Report_Time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""

        batch_data = []
        success_count = 0

        for i in range(count):
            try:
                report_date = self.txn_check_start + timedelta(
                    days=random.randint(0, (self.txn_check_end - self.txn_check_start).days)
                )

                suspicious_reasons = [
                    '交易金额与客户身份不符',
                    '频繁大额现金交易',
                    '短时间内多笔可疑交易',
                    '与高风险地区交易',
                    '交易模式异常',
                    '资金来源不明'
                ]

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
                batch_data.append(data)
                success_count += 1

            except Exception as e:
                print(f"[WARNING] 生成第{i}条可疑交易报告失败: {e}")
                continue

        if batch_data:
            try:
                self.cursor.executemany(sql, batch_data)
                self.conn.commit()
            except Exception as e:
                print(f"[ERROR] 批量插入可疑交易报告失败: {e}")
                return False

        print(f"    [OK] 可疑交易报告生成完成：{success_count} 条")
        return True

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
            ('tb_cash_convert', '现钞兑换'),
            ('tb_lwhc_log', '联网核查日志'),
            ('tb_lar_report', '大额交易报告'),
            ('tb_sus_report', '可疑交易报告'),
            ('tb_bank', '银行机构'),
            ('tb_settle_type', '业务类型')
        ]

        print("\n" + "="*60)
        print("AML300交易数据生成报告")
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

        return total_records

    def run_transaction_generation(self):
        """执行交易数据生成流程"""
        print("[INFO] 开始交易数据生成流程")
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

            # 3. 获取账户信息
            accounts = self.get_accounts()
            if len(accounts) == 0:
                print("[ERROR] 没有找到账户信息，请先生成账户数据")
                return False

            # 4. 生成账户交易数据
            if not self.generate_acc_transactions(accounts, 10000):
                return False

            # 5. 生成跨境交易数据
            if not self.generate_cross_border_transactions(accounts, 500):
                return False

            # 6. 生成其他业务数据
            if not self.generate_other_transactions(accounts):
                return False

            # 7. 生成综合报告
            self.generate_comprehensive_report()

            end_time = time.time()
            elapsed = end_time - start_time

            print(f"\n[SUCCESS] 交易数据生成完成！")
            print(f"[INFO] 总耗时: {elapsed:.2f} 秒")
            print("="*60)

            return True

        except Exception as e:
            print(f"[ERROR] 交易数据生成过程中发生错误: {e}")
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
    generator = FixedTransactionGenerator()
    success = generator.run_transaction_generation()

    if success:
        print("\n[SUCCESS] 交易数据生成成功完成！")
        print("请检查数据完整性并进行下一步测试。")
        sys.exit(0)
    else:
        print("\n[ERROR] 交易数据生成失败，请检查错误信息。")
        sys.exit(1)

if __name__ == "__main__":
    main()