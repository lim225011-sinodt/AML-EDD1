#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AML-EDD反洗钱数据库简化演示程序
版本: v1.0
创建时间: 2025-11-09
功能: 简化版本的AML-EDD数据库演示
"""

import sqlite3
import os
from datetime import datetime, timedelta

def create_sample_database():
    """创建示例数据库"""
    db_path = 'aml_edd_sample.db'

    # 删除旧数据库
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"删除旧数据库: {db_path}")

    # 连接数据库
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("创建数据表...")

    # 1. 机构表
    cursor.execute('''
        CREATE TABLE tb_bank (
            Head_no TEXT NOT NULL,
            Bank_code1 TEXT PRIMARY KEY,
            Bank_code2 TEXT UNIQUE,
            Bank_name TEXT NOT NULL,
            Bord_type TEXT NOT NULL DEFAULT '10'
        )
    ''')

    # 2. 业务类型表
    cursor.execute('''
        CREATE TABLE tb_settle_type (
            Head_no TEXT NOT NULL,
            Settle_type TEXT PRIMARY KEY,
            Name TEXT NOT NULL
        )
    ''')

    # 3. 个人客户表
    cursor.execute('''
        CREATE TABLE tb_cst_pers (
            Head_no TEXT NOT NULL,
            Bank_code1 TEXT NOT NULL,
            Cst_no TEXT PRIMARY KEY,
            Open_time TEXT NOT NULL,
            Acc_name TEXT NOT NULL,
            Cst_sex TEXT NOT NULL,
            Id_type TEXT NOT NULL,
            Id_no TEXT NOT NULL,
            Contact1 TEXT,
            Address1 TEXT,
            FOREIGN KEY (Bank_code1) REFERENCES tb_bank(Bank_code1)
        )
    ''')

    # 4. 企业客户表
    cursor.execute('''
        CREATE TABLE tb_cst_unit (
            Head_no TEXT NOT NULL,
            Bank_code1 TEXT NOT NULL,
            Cst_no TEXT PRIMARY KEY,
            Open_time TEXT NOT NULL,
            Acc_name TEXT NOT NULL,
            Rep_name TEXT,
            License TEXT,
            Industry TEXT,
            Reg_amt REAL,
            FOREIGN KEY (Bank_code1) REFERENCES tb_bank(Bank_code1)
        )
    ''')

    # 5. 账户表
    cursor.execute('''
        CREATE TABLE tb_acc (
            Head_no TEXT NOT NULL,
            Bank_code1 TEXT NOT NULL,
            Self_acc_name TEXT NOT NULL,
            Acc_state TEXT NOT NULL DEFAULT '11',
            Self_acc_no TEXT NOT NULL,
            Acc_type TEXT NOT NULL,
            Id_no TEXT NOT NULL,
            Cst_no TEXT NOT NULL,
            Open_time TEXT NOT NULL,
            PRIMARY KEY (Self_acc_no),
            FOREIGN KEY (Bank_code1) REFERENCES tb_bank(Bank_code1)
        )
    ''')

    # 6. 交易表
    cursor.execute('''
        CREATE TABLE tb_acc_txn (
            Date TEXT NOT NULL,
            Time TEXT NOT NULL,
            Self_bank_code TEXT NOT NULL,
            Acc_type TEXT NOT NULL,
            Cst_no TEXT NOT NULL,
            Self_acc_no TEXT NOT NULL,
            Cur TEXT NOT NULL,
            Org_amt REAL NOT NULL,
            Purpose TEXT
        )
    ''')

    # 7. 风险等级表
    cursor.execute('''
        CREATE TABLE tb_risk_new (
            Bank_code1 TEXT NOT NULL,
            Cst_no TEXT PRIMARY KEY,
            Acc_type TEXT NOT NULL,
            Risk_code TEXT NOT NULL,
            Time TEXT NOT NULL,
            Norm TEXT
        )
    ''')

    print("插入示例数据...")

    # 插入机构数据
    banks = [
        ('ABC001', '103100000019', '104100000004', '中国农业银行总行营业部', '10'),
        ('ABC001', '103100000027', '104100000012', '中国农业银行北京分行营业部', '10')
    ]
    cursor.executemany("INSERT INTO tb_bank VALUES (?, ?, ?, ?, ?)", banks)

    # 插入业务类型数据
    settle_types = [
        ('ABC001', 'ST001', '存款业务'),
        ('ABC001', 'ST002', '取款业务'),
        ('ABC001', 'ST003', '转账业务')
    ]
    cursor.executemany("INSERT INTO tb_settle_type VALUES (?, ?, ?)", settle_types)

    # 插入个人客户数据
    persons = [
        ('ABC001', '103100000019', 'P000001', '20200115', '张三', '11', '11', '110101199001011234', '13800138000', '北京市朝阳区金融街1号'),
        ('ABC001', '103100000019', 'P000002', '20200320', '李四', '12', '11', '310101199002022345', '13900139000', '上海市浦东新区陆家嘴2号'),
        ('ABC001', '103100000027', 'P000003', '20190710', '王五', '11', '11', '440103199303033456', '13700137000', '广州市天河区珠江新城3号')
    ]
    cursor.executemany("INSERT INTO tb_cst_pers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", persons)

    # 插入企业客户数据
    units = [
        ('ABC001', '103100000019', 'U00001', '20180512', '北京华强科技有限公司', '张华', '91110105MA01234567', '软件开发', 5000000.00),
        ('ABC001', '103100000027', 'U00002', '20191108', '上海金鼎贸易有限公司', '李金', '91310115MA09876543', '批发零售', 10000000.00)
    ]
    cursor.executemany("INSERT INTO tb_cst_unit VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", units)

    # 插入账户数据
    accounts = [
        ('ABC001', '103100000019', '张三', '11', 'ACC000001', '11', '110101199001011234', 'P000001', '20200115'),
        ('ABC001', '103100000019', '李四', '11', 'ACC000002', '11', '310101199002022345', 'P000002', '20200320'),
        ('ABC001', '103100000019', '北京华强科技有限公司', '11', 'BIZ00001', '12', '91110105MA01234567', 'U00001', '20180512')
    ]
    cursor.executemany("INSERT INTO tb_acc VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", accounts)

    # 插入交易数据
    transactions = [
        ('20250101', '091500', '103100000019', '11', 'P000001', 'ACC000001', 'CNY', 5000.00, '工资发放'),
        ('20250102', '143020', '103100000019', '11', 'P000001', 'ACC000001', 'CNY', 1200.00, '消费'),
        ('20250103', '101015', '103100000019', '11', 'P000002', 'ACC000002', 'CNY', 3000.00, '转账'),
        ('20250104', '161845', '103100000019', '12', 'U00001', 'BIZ00001', 'CNY', 50000.00, '采购付款')
    ]
    cursor.executemany("INSERT INTO tb_acc_txn VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", transactions)

    # 插入风险等级数据
    risks = [
        ('103100000019', 'P000001', '11', '12', '20241201', '客户综合评分正常，风险较低'),
        ('103100000019', 'P000002', '11', '11', '20241201', '客户交易金额较大，需要持续监控'),
        ('103100000019', 'P000003', '11', '12', '20241201', '客户评分正常'),
        ('103100000019', 'U00001', '12', '13', '20241201', '企业经营状况稳定，风险可控')
    ]
    cursor.executemany("INSERT INTO tb_risk_new VALUES (?, ?, ?, ?, ?, ?)", risks)

    # 提交事务
    conn.commit()

    # 生成统计报告
    print("\n" + "="*50)
    print("AML-EDD演示数据库创建完成")
    print("="*50)

    tables = [
        ('tb_bank', '机构对照表'),
        ('tb_settle_type', '业务类型对照表'),
        ('tb_cst_pers', '个人客户信息'),
        ('tb_cst_unit', '企业客户信息'),
        ('tb_acc', '账户信息'),
        ('tb_acc_txn', '交易记录'),
        ('tb_risk_new', '最新风险等级')
    ]

    for table_name, description in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"{description:15} : {count:6d} 条记录")

    # 查询示例
    print("\n查询示例:")
    print("\n1. 查询个人客户信息:")
    cursor.execute("SELECT Cst_no, Acc_name, Contact1, Address1 FROM tb_cst_pers LIMIT 3")
    for row in cursor.fetchall():
        print(f"  客户号: {row[0]}, 姓名: {row[1]}, 联系方式: {row[2]}, 地址: {row[3]}")

    print("\n2. 查询交易记录:")
    cursor.execute("""
        SELECT t.Date, t.Cur, t.Org_amt, t.Purpose, a.Self_acc_name
        FROM tb_acc_txn t
        JOIN tb_acc a ON t.Self_acc_no = a.Self_acc_no
        WHERE a.Acc_type = '11'
        LIMIT 5
    """)
    for row in cursor.fetchall():
        print(f"  日期: {row[0]}, 币种: {row[1]}, 金额: {row[2]:.2f}, 用途: {row[3]}, 户名: {row[4]}")

    print("\n3. 查询风险等级:")
    cursor.execute("""
        SELECT r.Risk_code, c.Acc_name, r.Time, r.Norm
        FROM tb_risk_new r
        JOIN tb_cst_pers c ON r.Cst_no = c.Cst_no AND r.Acc_type = '11'
        WHERE r.Acc_type = '11'
    """)
    for row in cursor.fetchall():
        risk_desc = {'10': '高风险', '11': '中高风险', '12': '中等风险', '13': '低风险'}
        print(f"  {row[1]}: {risk_desc.get(row[0], row[0])} - {row[3]}")

    # 关闭连接
    conn.close()

    file_size = os.path.getsize(db_path)
    print(f"\n数据库文件: {db_path}")
    print(f"文件大小: {file_size} 字节")
    print("="*50)
    print("数据库创建成功！")
    print("\n使用方法:")
    print("1. SQLite命令行: sqlite3 aml_edw_sample.db")
    print("2. Python代码: sqlite3.connect('aml_edw_sample.db')")
    print("3. 其他工具: DB Browser for SQLite")

if __name__ == "__main__":
    try:
        create_sample_database()
    except Exception as e:
        print(f"创建数据库失败: {e}")