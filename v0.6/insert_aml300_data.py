#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
向AML300数据库插入测试数据
"""

import mysql.connector
import sys
import random
from datetime import datetime, timedelta

def insert_sample_data():
    """插入示例数据到AML300数据库"""
    print("=== 向 AML300 数据库插入测试数据 ===\n")

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

        # 1. 插入机构数据
        print("插入机构数据...")
        banks = [
            ('ABC001', '103100000019', '104100000004', '中国农业银行总行营业部', '10'),
            ('ABC001', '103100000027', '104100000012', '中国农业银行北京分行营业部', '10'),
            ('ABC001', '103100000035', '104100000020', '中国农业银行上海分行营业部', '10'),
            ('ABC001', '103100000043', '104100000038', '中国农业银行广东分行营业部', '10'),
            ('ABC001', '103100000050', '104100000045', '中国农业银行深圳分行营业部', '10')
        ]
        cursor.executemany("INSERT INTO tb_bank VALUES (%s, %s, %s, %s, %s)", banks)
        conn.commit()
        print(f"插入 {len(banks)} 条机构记录")

        # 2. 插入业务类型数据
        print("插入业务类型数据...")
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
            ('ABC001', 'ST010', '外汇买卖')
        ]
        cursor.executemany("INSERT INTO tb_settle_type VALUES (%s, %s, %s)", settle_types)
        conn.commit()
        print(f"插入 {len(settle_types)} 条业务类型记录")

        # 3. 插入个人客户数据（少量测试数据）
        print("插入个人客户数据...")
        persons = [
            ('ABC001', '103100000019', 'P000001', '20200115', None, '张三', '11', 'CHN', '11', '110101199001011234', '99991231', '软件工程师', 120000.00, '13800138000', '13800138001', '13800138002', '北京市朝阳区金融街1号', None, None, '北京华强科技有限公司', '个人网银系统'),
            ('ABC001', '103100000019', 'P000002', '20200320', None, '李四', '12', 'CHN', '11', '310101199002022345', '99991231', '教师', 80000.00, '13900139000', None, None, '上海市浦东新区陆家嘴2号', None, None, '上海金鼎贸易公司', '柜面系统'),
            ('ABC001', '103100000027', 'P000003', '20190710', None, '王五', '11', 'CHN', '11', '440103199303033456', '99991231', '医生', 150000.00, '13700137000', None, None, '广州市天河区珠江新城3号', None, None, '广州医疗中心', '个人网银系统'),
            ('ABC001', '103100000027', 'P000004', '20210105', None, '赵六', '11', 'CHN', '11', '330102199404044567', '99991231', '金融分析师', 200000.00, '13600136000', None, None, '杭州市西湖区文三路4号', None, None, '杭州金融科技有限公司', '个人网银系统'),
            ('ABC001', '103100000035', 'P000005', '20200812', None, '钱七', '12', 'CHN', '11', '440304199505055678', '99991231', '会计', 90000.00, '13500135000', None, None, '深圳市福田区华强北路5号', None, None, '深圳财务管理公司', '柜面系统')
        ]
        cursor.executemany("INSERT INTO tb_cst_pers (Head_no, Bank_code1, Cst_no, Open_time, Close_time, Acc_name, Cst_sex, Nation, Id_type, Id_no, Id_deadline, Occupation, Income, Contact1, Contact2, Contact3, Address1, Address2, Address3, Company, Sys_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", persons)
        conn.commit()
        print(f"插入 {len(persons)} 条个人客户记录")

        # 4. 插入企业客户数据
        print("插入企业客户数据...")
        units = [
            ('ABC001', '103100000019', 'U00001', '20180512', '北京华强科技有限公司', '张华', '李明', '91110105MA01234567', '99991231', '软件开发', 5000000.00, 'RMB', '企业网银系统'),
            ('ABC001', '103100000027', 'U00002', '20191108', '上海金鼎贸易有限公司', '李金', '王红', '91310115MA09876543', '99991231', '批发零售', 10000000.00, 'RMB', '对公业务系统'),
            ('ABC001', '103100000035', 'U00003', '20200215', '广州创新信息技术有限公司', '王创新', '赵技术', '91440101MA12345678', '99991231', '信息技术', 3000000.00, 'RMB', '企业网银系统'),
            ('ABC001', '103100000043', 'U00004', '20210320', '深圳智能制造股份有限公司', '刘制造', '陈智能', '91440300MA23456789', '99991231', '制造业', 50000000.00, 'RMB', '对公业务系统')
        ]
        cursor.executemany("INSERT INTO tb_cst_unit VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", units)
        conn.commit()
        print(f"插入 {len(units)} 条企业客户记录")

        # 5. 插入账户数据
        print("插入账户数据...")
        accounts = [
            ('ABC001', '103100000019', '张三', '11', 'ACC000001', None, '11', '11', '110101199001011234', 'P000001', '20200115', None, '12', None, None),
            ('ABC001', '103100000019', '李四', '11', 'ACC000002', None, '11', '11', '310101199002022345', 'P000002', '20200320', None, '12', None, None),
            ('ABC001', '103100000019', '王五', '11', 'ACC000003', 'CARD000003', '11', '12', '440103199303033456', 'P000003', '20190710', None, '12', None, None),
            ('ABC001', '103100000019', '北京华强科技有限公司', '11', 'BIZ00001', None, '12', None, '91110105MA01234567', 'U00001', '20180512', None, '12', None, None),
            ('ABC001', '103100000027', '上海金鼎贸易有限公司', '11', 'BIZ00002', None, '12', None, '91310115MA09876543', 'U00002', '20191108', None, '12', None, None)
        ]
        cursor.executemany("INSERT INTO tb_acc VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", accounts)
        conn.commit()
        print(f"插入 {len(accounts)} 条账户记录")

        # 6. 插入交易记录
        print("插入交易记录...")
        transactions = []
        purposes = ['工资发放', '消费', '转账', '采购付款', '投资收益', '货款结算', '服务费', '租金支付']

        for i in range(50):  # 插入50条交易记录
            trans_date = (datetime.now() - timedelta(days=random.randint(0, 90))).strftime('%Y%m%d')
            trans_time = datetime.now().replace(hour=random.randint(8, 18), minute=random.randint(0, 59), second=random.randint(0, 59)).strftime('%H%M%S')

            transaction = (
                trans_date,  # Date
                trans_time,  # Time
                '103100000019',  # Self_bank_code
                '11',  # Acc_type
                'P000001',  # Cst_no
                '110101199001011234',  # Id_no
                'ACC000001',  # Self_acc_no
                None,  # Card_no
                None,  # Part_acc_no
                None,  # Part_acc_name
                random.choice(['10', '11']),  # Lend_flag
                '11',  # Tsf_flag
                'CNY',  # Cur
                round(random.uniform(100, 10000), 2),  # Org_amt
                round(random.uniform(100/7, 10000/7), 2),  # Usd_amt
                round(random.uniform(100, 10000), 2),  # Rmb_amt
                round(random.uniform(10000, 100000), 2),  # Balance
                '12',  # Agency_flag
                '10',  # Reverse_flag
                random.choice(purposes),  # Purpose
                '12',  # Bord_flag
                None,  # Nation
                random.choice(['11', '12', '13']),  # Bank_flag
                None,  # Ip_code
                None,  # Atm_code
                None,  # Bank_code
                None,  # Mac_info
                'ST001',  # Settle_type
                f'TXN{trans_date}{i+1:03d}'  # Ticd
            )
            transactions.append(transaction)

        cursor.executemany("""
            INSERT INTO tb_acc_txn (
                Date, Time, Self_bank_code, Acc_type, Cst_no, Id_no,
                Self_acc_no, Card_no, Part_acc_no, Part_acc_name, Lend_flag,
                Tsf_flag, Cur, Org_amt, Usd_amt, Rmb_amt, Balance,
                Agency_flag, Reverse_flag, Purpose, Bord_flag, Nation,
                Bank_flag, Ip_code, Atm_code, Bank_code, Mac_info,
                Settle_type, Ticd
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, transactions)
        conn.commit()
        print(f"插入 {len(transactions)} 条交易记录")

        # 7. 插入风险等级数据
        print("插入风险等级数据...")
        risks = [
            ('103100000019', 'P000001', '张三', '110101199001011234', '11', '12', '20241201', '客户综合评分正常，风险较低'),
            ('103100000019', 'P000002', '李四', '310101199002022345', '11', '11', '20241201', '客户交易金额较大，需要持续监控'),
            ('103100000019', 'P000003', '王五', '440103199303033456', '11', '12', '20241201', '客户评分正常'),
            ('103100000019', 'P000004', '赵六', '330102199404044567', '11', '12', '20241201', '客户综合评分正常'),
            ('103100000019', 'P000005', '钱七', '440304199505055678', '11', '13', '20241201', '客户评分正常，风险低'),
            ('103100000019', 'U00001', '北京华强科技有限公司', '91110105MA01234567', '12', '13', '20241201', '企业经营状况稳定，风险可控'),
            ('103100000027', 'U00002', '上海金鼎贸易有限公司', '91310115MA09876543', '12', '12', '20241201', '企业经营正常，风险中等'),
            ('103100000035', 'U00003', '广州创新信息技术有限公司', '91440101MA12345678', '12', '11', '20241201', '新成立企业，需要加强监控'),
            ('103100000043', 'U00004', '深圳智能制造股份有限公司', '91440300MA23456789', '12', '12', '20241201', '企业经营状况稳定')
        ]
        cursor.executemany("INSERT INTO tb_risk_new VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", risks)
        conn.commit()
        print(f"插入 {len(risks)} 条风险等级记录")

        # 插入一些历史风险等级数据
        print("插入历史风险等级数据...")
        historical_risks = [
            ('103100000019', 'P000001', '张三', '110101199001011234', '11', '12', '20240601', '前期风险评估结果'),
            ('103100000019', 'P000002', '李四', '310101199002022345', '11', '12', '20240601', '定期风险复评结果'),
            ('103100000019', 'U00001', '北京华强科技有限公司', '91110105MA01234567', '12', '13', '20240601', '前期风险等级调整记录')
        ]
        cursor.executemany("INSERT INTO tb_risk_his VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", historical_risks)
        conn.commit()
        print(f"插入 {len(historical_risks)} 条历史风险等级记录")

        # 验证数据
        print("\n验证插入的数据:")
        tables_to_check = [
            ('tb_bank', '机构对照表'),
            ('tb_settle_type', '业务类型对照表'),
            ('tb_cst_pers', '个人客户信息'),
            ('tb_cst_unit', '企业客户信息'),
            ('tb_acc', '账户信息'),
            ('tb_acc_txn', '交易记录'),
            ('tb_risk_new', '最新风险等级'),
            ('tb_risk_his', '历史风险等级')
        ]

        total_records = 0
        for table_name, description in tables_to_check:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            total_records += count
            print(f"  {description}: {count:6d} 条记录")

        print(f"\n总计: {total_records:,} 条记录")

        cursor.close()
        conn.close()

        print("\n[SUCCESS] 测试数据插入完成!")
        print("AML300数据库现已准备就绪，包含:")
        print("- 基础数据：机构、业务类型")
        print("- 客户数据：个人客户、企业客户")
        print("- 账户数据：账户信息")
        print("- 交易数据：交易记录")
        print("- 风险数据：风险等级评估")

        return True

    except Exception as e:
        print(f"[ERROR] 插入数据失败: {e}")
        return False

if __name__ == "__main__":
    success = insert_sample_data()
    sys.exit(0 if success else 1)