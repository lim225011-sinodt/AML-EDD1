#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修正版：为AML300数据库15张表生成完整的模拟数据（基于实际表结构）
"""

import mysql.connector
import random
import string
from datetime import datetime, timedelta
import sys

class MockDataGeneratorFixed:
    """修正版模拟数据生成器"""

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

        print("成功连接到AML300数据库，准备生成模拟数据（修正版）")

    def generate_id_number(self):
        """生成身份证号码"""
        area_code = random.choice(['110101', '310101', '440103', '330102', '510104'])
        birth_date = f"{random.randint(1960, 2000):04d}{random.randint(1, 12):02d}{random.randint(1, 28):02d}"
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

    def generate_random_amount(self, min_val=100, max_val=1000000):
        """生成随机金额"""
        return round(random.uniform(min_val, max_val), 2)

    def create_banks(self):
        """创建银行信息"""
        print("创建银行信息表...")
        bank_data = []
        for i, bank_name in enumerate(self.bank_names, 1):
            bank_code1 = f"BK{str(i).zfill(3)}"
            bank_code2 = f"104{i:04d}"
            head_no = f"H{i:03d}"

            bank_data.append((
                head_no, bank_code1, bank_code2, bank_name,
                random.choice(['11', '12'])  # Bord_type
            ))

        self.cursor.executemany("""
            INSERT INTO tb_bank (Head_no, Bank_code1, Bank_code2, Bank_name, Bord_type)
            VALUES (%s, %s, %s, %s, %s)
        """, bank_data)
        print(f"已创建 {len(bank_data)} 条银行记录")

    def create_settle_types(self):
        """创建业务类型"""
        print("创建业务类型表...")
        settle_types = [
            ('ST001', '现金存款'),
            ('ST002', '现金取款'),
            ('ST003', '转账汇款'),
            ('ST004', '跨境汇款'),
            ('ST005', 'POS消费'),
            ('ST006', '网银支付'),
            ('ST007', '账户查询'),
            ('ST008', '理财产品')
        ]

        settle_data = []
        head_no = "H001"
        for settle_code, name in settle_types:
            settle_data.append((head_no, settle_code, name))

        self.cursor.executemany("""
            INSERT INTO tb_settle_type (Head_no, Settle_type, Name)
            VALUES (%s, %s, %s)
        """, settle_data)
        print(f"已创建 {len(settle_data)} 条业务类型记录")

    def create_individual_customers(self, count=10):
        """创建个人客户"""
        print(f"创建个人客户表 ({count}条)...")
        cst_pers_data = []

        for i in range(1, count + 1):
            surname = random.choice(self.surnames)
            given_name = random.choice(self.given_names)
            name = surname + given_name
            id_no = self.generate_id_number()

            cst_pers_data.append((
                f"H{i:03d}",  # Head_no
                f"BK{str(random.randint(1, 6)).zfill(3)}",  # Bank_code1
                f"P{i:06d}",  # Cst_no
                self.generate_random_date('2020-01-01', '2024-12-31').replace('-', ''),  # Open_time
                None,  # Close_time
                name,  # Acc_name
                random.choice(['11', '12']),  # Cst_sex
                'CHN',  # Nation
                '11',  # Id_type (身份证)
                id_no,  # Id_no
                '20300101',  # Id_deadline
                random.choice(['工程师', '教师', '医生', '销售', '管理', '其他']),  # Occupation
                self.generate_random_amount(50000, 500000),  # Income
                f"138{random.randint(10000000, 99999999)}",  # Contact1
                None, None,  # Contact2, Contact3
                f"北京市朝阳区{i}号",  # Address1
                None, None,  # Address2, Address3
                f"某科技公司",  # Company
                f"系统{i:03d}"  # Sys_name
            ))

        self.cursor.executemany("""
            INSERT INTO tb_cst_pers (Head_no, Bank_code1, Cst_no, Open_time, Close_time, Acc_name,
                                    Cst_sex, Nation, Id_type, Id_no, Id_deadline, Occupation, Income,
                                    Contact1, Contact2, Contact3, Address1, Address2, Address3, Company, Sys_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, cst_pers_data[:10])  # 先测试10条
        print(f"已创建 {len(cst_pers_data[:10])} 条个人客户记录")

    def create_corporate_customers(self, count=5):
        """创建企业客户"""
        print(f"创建企业客户表 ({count}条)...")
        cst_unit_data = []

        for i in range(1, count + 1):
            company_type = random.choice(self.company_types)
            suffix = random.choice(self.company_suffixes)
            name = f"{random.choice(['华', '中', '国', '民', '金', '信', '安', '达'])}{random.choice(['兴', '发', '展', '达', '盛', '通', '汇', '融'])}{company_type}{suffix}"

            cst_unit_data.append((
                f"H{100+i:03d}",  # Head_no
                f"BK{str(random.randint(1, 6)).zfill(3)}",  # Bank_code1
                f"U{i:06d}",  # Cst_no
                self.generate_random_date('2020-01-01', '2024-12-31').replace('-', ''),  # Open_time
                name,  # Acc_name
                f"法人{random.randint(1, 999)}",  # Rep_name
                f"经办人{random.randint(1, 999)}",  # Ope_name
                f"LICENSE{random.randint(10000000, 99999999)}",  # License
                '20300101',  # Id_deadline
                random.choice(['科技', '贸易', '制造', '服务', '金融', '教育']),  # Industry
                self.generate_random_amount(1000000, 50000000),  # Reg_amt
                'CNY',  # Reg_amt_code
                f"企业系统{i:03d}"  # Sys_name
            ))

        self.cursor.executemany("""
            INSERT INTO tb_cst_unit (Head_no, Bank_code1, Cst_no, Open_time, Acc_name, Rep_name,
                                    Ope_name, License, Id_deadline, Industry, Reg_amt, Reg_amt_code, Sys_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, cst_unit_data)
        print(f"已创建 {len(cst_unit_data[:5])} 条企业客户记录")

    def create_accounts(self, count=10):
        """创建账户信息"""
        print(f"创建账户表 ({count}条)...")
        acc_data = []

        # 获取客户编号
        self.cursor.execute("SELECT Cst_no, Acc_name FROM tb_cst_pers LIMIT 600")
        pers_customers = self.cursor.fetchall()

        self.cursor.execute("SELECT Cst_no, Acc_name FROM tb_cst_unit LIMIT 200")
        unit_customers = self.cursor.fetchall()

        all_customers = pers_customers + unit_customers

        for i in range(1, count + 1):
            if i <= len(all_customers):
                cst_no, acc_name = all_customers[i-1]
            else:
                cst_no, acc_name = all_customers[0]

            acc_no = f"6228{random.randint(1000000000000000, 9999999999999999)}"

            acc_data.append((
                f"H{i:03d}",  # Head_no
                f"BK{str(random.randint(1, 6)).zfill(3)}",  # Bank_code1
                acc_name,  # Self_acc_name
                random.choice(['11', '12']),  # Acc_state
                acc_no,  # Self_acc_no
                f"6225{random.randint(1000000000000000, 9999999999999999)}",  # Card_no
                random.choice(['11', '12']),  # Acc_type
                random.choice(['21', '22']),  # Acc_type1
                self.generate_id_number(),  # Id_no
                cst_no,  # Cst_no
                self.generate_random_date('2020-01-01', '2024-12-31').replace('-', ''),  # Open_time
                None,  # Close_time
                None, None, None  # Agency_flag, Acc_flag, Fixed_flag
            ))

        self.cursor.executemany("""
            INSERT INTO tb_acc (Head_no, Bank_code1, Self_acc_name, Acc_state, Self_acc_no, Card_no,
                               Acc_type, Acc_type1, Id_no, Cst_no, Open_time, Close_time,
                               Agency_flag, Acc_flag, Fixed_flag)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, acc_data)
        print(f"已创建 {len(acc_data)} 条账户记录")

    def create_risk_levels(self):
        """创建风险等级信息"""
        print("创建风险等级表...")

        # 获取所有客户
        self.cursor.execute("SELECT Cst_no FROM tb_cst_pers")
        pers_customers = [row[0] for row in self.cursor.fetchall()]

        self.cursor.execute("SELECT Cst_no FROM tb_cst_unit")
        unit_customers = [row[0] for row in self.cursor.fetchall()]

        all_customers = pers_customers + unit_customers

        # 获取银行代码
        self.cursor.execute("SELECT Bank_code1 FROM tb_bank")
        banks = [row[0] for row in self.cursor.fetchall()]

        # 创建最新风险等级
        risk_new_data = []
        for cst_no in all_customers:
            risk_code = random.choice(['01', '02', '03', '04'])  # 风险等级代码
            bank_code = random.choice(banks)

            risk_new_data.append((
                bank_code, cst_no,
                f"客户{cst_no}",  # Self_acc_name
                self.generate_id_number(),  # Id_no
                random.choice(['11', '12']),  # Acc_type
                risk_code,  # Risk_code
                self.generate_random_date('2024-01-01', '2024-12-31').replace('-', ''),  # Time
                f"风险等级{risk_code}的详细说明"  # Norm
            ))

        self.cursor.executemany("""
            INSERT INTO tb_risk_new (Bank_code1, Cst_no, Self_acc_name, Id_no, Acc_type,
                                   Risk_code, Time, Norm)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, risk_new_data)
        print(f"已创建 {len(risk_new_data)} 条最新风险等级记录")

        # 创建历史风险等级（每个客户2-3条历史记录）
        risk_his_data = []
        for cst_no in all_customers[:500]:  # 限制数量避免过多
            bank_code = random.choice(banks)
            history_count = random.randint(2, 3)
            for i in range(history_count):
                risk_code = random.choice(['01', '02', '03', '04'])

                risk_his_data.append((
                    bank_code, cst_no,
                    f"客户{cst_no}",  # Self_acc_name
                    self.generate_id_number(),  # Id_no
                    random.choice(['11', '12']),  # Acc_type
                    risk_code,  # Risk_code
                    self.generate_random_date('2022-01-01', '2023-12-31').replace('-', ''),  # Time
                    f"历史风险等级{risk_code}的说明"  # Norm
                ))

        self.cursor.executemany("""
            INSERT INTO tb_risk_his (Bank_code1, Cst_no, Self_acc_name, Id_no, Acc_type,
                                   Risk_code, Time, Norm)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, risk_his_data)
        print(f"已创建 {len(risk_his_data)} 条历史风险等级记录")

    def generate_all_data(self):
        """生成所有模拟数据"""
        try:
            print("=== 开始生成AML300数据库模拟数据（修正版）===\n")

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

            # 提交事务
            self.conn.commit()
            print(f"\n[SUCCESS] 基础模拟数据生成完成！")

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
            print(f"\n注意：交易表数据量较大，建议分批生成。目前已生成核心表的基础数据。")

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
    generator = MockDataGeneratorFixed()
    success = generator.generate_all_data()
    generator.close()
    sys.exit(0 if success else 1)