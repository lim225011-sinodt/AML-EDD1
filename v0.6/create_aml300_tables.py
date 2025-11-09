#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
创建AML300数据库表结构
"""

import mysql.connector
import sys
import time

def create_aml300_tables():
    """创建AML300数据库表"""
    print("=== 创建 AML300 数据库表结构 ===\n")

    try:
        # 连接MySQL服务器
        conn = mysql.connector.connect(
            host='101.42.102.9',
            port=3306,
            user='root',
            password='Bancstone123!',
            charset='utf8mb4'
        )

        cursor = conn.cursor()

        # 创建数据库
        cursor.execute("DROP DATABASE IF EXISTS AML300")
        cursor.execute("CREATE DATABASE AML300 CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        cursor.execute("USE AML300")
        conn.commit()
        print("创建并切换到 AML300 数据库")

        # 基础设置
        cursor.execute("SET NAMES utf8mb4")
        cursor.execute("SET sql_mode = 'STRICT_ALL_TABLES'")
        conn.commit()

        # 1) 机构对照表
        print("创建机构对照表...")
        cursor.execute("""
            CREATE TABLE tb_bank (
              Head_no       VARCHAR(20)  NOT NULL COMMENT '报告机构编码',
              Bank_code1    VARCHAR(20)  NOT NULL COMMENT '机构网点代码（PBoC机构网点代码口径）',
              Bank_code2    VARCHAR(16)  NULL COMMENT '金融机构编码（14位金融标准化编码）',
              Bank_name     VARCHAR(120) NOT NULL COMMENT '机构名称（与Bank_code1/Bank_code2对应）',
              Bord_type     CHAR(2)      NOT NULL DEFAULT '10' COMMENT '跨境标识：10境内；11境外',
              PRIMARY KEY (Bank_code1),
              UNIQUE KEY uk_tb_bank_code2 (Bank_code2)
            ) COMMENT='机构对照表'
        """)

        # 2) 业务类型对照表
        print("创建业务类型对照表...")
        cursor.execute("""
            CREATE TABLE tb_settle_type (
              Head_no     VARCHAR(20)  NOT NULL COMMENT '报告机构编码',
              Settle_type VARCHAR(20)  NOT NULL COMMENT '业务类型编码（系统内数据字典编码）',
              Name        VARCHAR(100) NOT NULL COMMENT '业务名称（编码对应中文名）',
              PRIMARY KEY (Settle_type)
            ) COMMENT='业务类型对照表'
        """)

        # 3) 个人客户信息表
        print("创建个人客户信息表...")
        cursor.execute("""
            CREATE TABLE tb_cst_pers (
              Head_no      VARCHAR(20)  NOT NULL COMMENT '报告机构编码',
              Bank_code1   VARCHAR(20)  NOT NULL COMMENT '客户号归属机构网点代码，指向tb_bank',
              Cst_no       VARCHAR(50)  NOT NULL COMMENT '客户号',
              Open_time    CHAR(8)      NOT NULL COMMENT '创建日期YYYYMMDD',
              Close_time   CHAR(8)      NULL COMMENT '结束日期YYYYMMDD（销号日）',
              Acc_name     VARCHAR(40)  NOT NULL COMMENT '客户姓名',
              Cst_sex      CHAR(2)      NOT NULL COMMENT '性别：11男；12女',
              Nation       VARCHAR(20)  NOT NULL COMMENT '国籍/地区（GB/T2659-2000缩写）',
              Id_type      CHAR(2)      NOT NULL COMMENT '证件种类（11身份证…14护照…19其他）',
              Id_no        VARCHAR(50)  NOT NULL COMMENT '证件号码',
              Id_deadline  CHAR(8)      NOT NULL COMMENT '证件有效期至（长期99991231）',
              Occupation   VARCHAR(80)  NULL COMMENT '职业（如使用代码需转文字）',
              Income       DECIMAL(16,2) NULL COMMENT '年收入（元）',
              Contact1     VARCHAR(20)  NULL COMMENT '联系方式1（手机或固话）',
              Contact2     VARCHAR(20)  NULL COMMENT '联系方式2',
              Contact3     VARCHAR(20)  NULL COMMENT '联系方式3',
              Address1     VARCHAR(500) NULL COMMENT '住所地/工作单位地址',
              Address2     VARCHAR(500) NULL COMMENT '其他住所/单位地址1',
              Address3     VARCHAR(500) NULL COMMENT '其他住所/单位地址2',
              Company      VARCHAR(120) NULL COMMENT '工作单位名称',
              Sys_name     VARCHAR(120) NULL COMMENT '采集系统名称（多系统以;分隔）',
              PRIMARY KEY (Cst_no),
              KEY idx_cstpers_bank (Bank_code1),
              KEY idx_cstpers_id (Id_no),
              CONSTRAINT fk_cstpers_bank FOREIGN KEY (Bank_code1) REFERENCES tb_bank(Bank_code1)
            ) COMMENT='存量个人客户身份信息表'
        """)

        # 4) 企业客户信息表
        print("创建企业客户信息表...")
        cursor.execute("""
            CREATE TABLE tb_cst_unit (
              Head_no      VARCHAR(20)  NOT NULL COMMENT '报告机构编码',
              Bank_code1   VARCHAR(20)  NOT NULL COMMENT '客户号归属机构网点代码，指向tb_bank',
              Cst_no       VARCHAR(50)  NOT NULL COMMENT '客户号',
              Open_time    CHAR(8)      NOT NULL COMMENT '创建日期YYYYMMDD',
              Acc_name     VARCHAR(120) NOT NULL COMMENT '客户名称（单位名称）',
              Rep_name     VARCHAR(40)  NULL COMMENT '法定代表人/负责人姓名',
              Ope_name     VARCHAR(40)  NULL COMMENT '授权经办人姓名',
              License      VARCHAR(50)  NULL COMMENT '单位证照号（交易等表对照"单位客户按License"）',
              Id_deadline  CHAR(8)      NULL COMMENT '证照有效期至（长期99991231）',
              Industry     VARCHAR(80)  NULL COMMENT '行业（如代码需转文字）',
              Reg_amt      DECIMAL(18,2) NULL COMMENT '注册资本金',
              Reg_amt_code CHAR(3)      NULL COMMENT '注册资本金币种（RMB/USD等国标）',
              Sys_name     VARCHAR(120) NULL COMMENT '采集系统名称（多系统以;分隔）',
              PRIMARY KEY (Cst_no),
              KEY idx_cstunit_bank (Bank_code1),
              KEY idx_cstunit_repname (Rep_name),
              KEY idx_cstunit_opename (Ope_name),
              KEY idx_cstunit_license (License),
              CONSTRAINT fk_cstunit_bank FOREIGN KEY (Bank_code1) REFERENCES tb_bank(Bank_code1)
            ) COMMENT='存量单位客户身份信息表'
        """)

        # 5) 账户主档
        print("创建账户主档...")
        cursor.execute("""
            CREATE TABLE tb_acc (
              Head_no       VARCHAR(20)  NOT NULL DEFAULT 'ABC001' COMMENT '报告机构编码',
              Bank_code1    VARCHAR(20)  NOT NULL COMMENT '开户行机构网点代码，指向tb_bank',
              Self_acc_name VARCHAR(120) NOT NULL COMMENT '账户户名（个人/单位）',
              Acc_state     CHAR(2)      NOT NULL DEFAULT '11' COMMENT '账户状态：11正常；12其他',
              Self_acc_no   VARCHAR(40)  NOT NULL PRIMARY KEY COMMENT '账号（若仅有卡号则同卡号）',
              Card_no       VARCHAR(40)  NULL COMMENT '卡号（可1账号多卡，多行表示映射）',
              Acc_type      CHAR(2)      NOT NULL COMMENT '公私标识：11个人；12单位',
              Acc_type1     CHAR(2)      NULL COMMENT '个人账户种类：11 I类；12 II类；13 III类；14信用卡',
              Id_no         VARCHAR(50)  NOT NULL COMMENT '证件号：个人按表3; 单位按表4 License口径',
              Cst_no        VARCHAR(50)  NOT NULL COMMENT '客户号',
              Open_time     CHAR(8)      NOT NULL COMMENT '开户日期',
              Close_time    CHAR(8)      NULL COMMENT '销户日期',
              Agency_flag   CHAR(2)      NULL COMMENT '开户是否代理：11代理；12本人',
              Acc_flag      CHAR(2)      NULL COMMENT '账户性质/是否特殊',
              Fixed_flag    CHAR(2)      NULL COMMENT '是否定期/冻结标志',
              KEY idx_acc_bank (Bank_code1),
              KEY idx_acc_cst (Cst_no),
              CONSTRAINT fk_acc_bank FOREIGN KEY (Bank_code1) REFERENCES tb_bank(Bank_code1)
            ) COMMENT='符合特定条件的银行账户信息表（主档）'
        """)

        # 6) 交易记录表
        print("创建交易记录表...")
        cursor.execute("""
            CREATE TABLE tb_acc_txn (
              Date          CHAR(8)      NOT NULL COMMENT '交易日期YYYYMMDD',
              Time          CHAR(6)      NOT NULL COMMENT '交易时间HHMMSS',
              Self_bank_code VARCHAR(20) NOT NULL COMMENT '交易行代码=tb_bank.Bank_code1',
              Acc_type      CHAR(2)      NOT NULL COMMENT '公私标识：11个人；12单位',
              Cst_no        VARCHAR(50)  NOT NULL COMMENT '客户号',
              Id_no         VARCHAR(50)  NOT NULL COMMENT '证件号（个人按表3；单位按表4 License）',
              Self_acc_no   VARCHAR(40)  NOT NULL COMMENT '我方账号（不能为空）',
              Card_no       VARCHAR(40)  NULL COMMENT '我方卡号',
              Part_acc_no   VARCHAR(60)  NULL COMMENT '对手账号/卡号',
              Part_acc_name VARCHAR(120) NULL COMMENT '对手账户名称',
              Lend_flag     CHAR(2)      NOT NULL COMMENT '收付标识：10收；11付（客户角度）',
              Tsf_flag      CHAR(2)      NOT NULL COMMENT '现金/转账：10现金；11转账（客户角度）',
              Cur           CHAR(3)      NOT NULL COMMENT '币种（GB/T12406-2008）',
              Org_amt       DECIMAL(16,2) NOT NULL COMMENT '原币种交易金额',
              Usd_amt       DECIMAL(16,2) NOT NULL COMMENT '折美元交易金额',
              Rmb_amt       DECIMAL(16,2) NOT NULL COMMENT '折人民币交易金额',
              Balance       DECIMAL(16,2) NULL COMMENT '交易后账户原币种余额',
              Agency_flag   CHAR(2)      NULL COMMENT '是否代理：11代理；12本人',
              Reverse_flag  CHAR(2)      NOT NULL DEFAULT '10' COMMENT '冲账标识：10否；11是',
              Purpose       VARCHAR(120) NULL COMMENT '摘要/用途',
              Bord_flag     CHAR(2)      NULL DEFAULT '12' COMMENT '跨境标识：11是；12否',
              Nation        VARCHAR(20)  NULL COMMENT '对方国家/地区（跨境=11时填，GB/T2659-2000）',
              Bank_flag     CHAR(2)      NULL COMMENT '交易方式：11网银；12手机；13柜面；14ATM；15其他',
              Ip_code       VARCHAR(17)  NULL COMMENT 'IP地址（网银等口径）',
              Atm_code      VARCHAR(30)  NULL COMMENT 'ATM机具编号（ATM时）',
              Bank_code     VARCHAR(20)  NULL COMMENT 'ATM机具所属行行号=tb_bank.Bank_code1',
              Mac_info      VARCHAR(17)  NULL COMMENT 'MAC/IMEI（PC/移动端）',
              Settle_type   VARCHAR(20)  NULL COMMENT '业务类型编码（指向tb_settle_type）',
              Ticd          VARCHAR(40)  NULL COMMENT '业务流水号',
              PRIMARY KEY (Date, Time, Self_acc_no, Lend_flag, Tsf_flag),
              KEY idx_txn_bank (Self_bank_code),
              CONSTRAINT fk_txn_bank FOREIGN KEY (Self_bank_code) REFERENCES tb_bank(Bank_code1),
              CONSTRAINT fk_txn_settle FOREIGN KEY (Settle_type) REFERENCES tb_settle_type(Settle_type)
            ) COMMENT='基于客户账户的交易数据表（非贷记账户）'
        """)

        # 7) 风险等级历史
        print("创建风险等级历史表...")
        cursor.execute("""
            CREATE TABLE tb_risk_his (
              Bank_code1 VARCHAR(20) NOT NULL COMMENT '机构网点代码=tb_bank.Bank_code1',
              Cst_no     VARCHAR(50) NOT NULL COMMENT '客户号',
              Self_acc_name VARCHAR(120) NULL COMMENT '客户名称',
              Id_no      VARCHAR(50) NOT NULL COMMENT '证件号：个人表3；单位表4 License',
              Acc_type   CHAR(2)     NOT NULL COMMENT '公私：11个人；12单位',
              Risk_code  CHAR(2)     NOT NULL COMMENT '风险等级（10高…14低，或按五级扩展）',
              Time       CHAR(8)     NOT NULL COMMENT '划分日期（若"系统先评再人工确认"，填确认日）',
              Norm       VARCHAR(500) NULL COMMENT '划分依据/评分分值或评定理由',
              PRIMARY KEY (Cst_no, Time),
              CONSTRAINT fk_riskhis_bank FOREIGN KEY (Bank_code1) REFERENCES tb_bank(Bank_code1)
            ) COMMENT='检查期内历次风险等级记录'
        """)

        # 8) 最新风险等级
        print("创建最新风险等级表...")
        cursor.execute("""
            CREATE TABLE tb_risk_new (
              Bank_code1 VARCHAR(20) NOT NULL COMMENT '机构网点代码=tb_bank.Bank_code1',
              Cst_no     VARCHAR(50) NOT NULL COMMENT '客户号',
              Self_acc_name VARCHAR(120) NULL COMMENT '客户名称',
              Id_no      VARCHAR(50) NOT NULL COMMENT '证件号',
              Acc_type   CHAR(2)     NOT NULL COMMENT '公私：11个人；12单位',
              Risk_code  CHAR(2)     NOT NULL COMMENT '风险等级（同口径）',
              Time       CHAR(8)     NOT NULL COMMENT '最新一次划分日期',
              Norm       VARCHAR(500) NULL COMMENT '划分依据/理由',
              PRIMARY KEY (Cst_no),
              KEY idx_risknew_combo1 (Time, Id_no),
              CONSTRAINT fk_risknew_bank FOREIGN KEY (Bank_code1) REFERENCES tb_bank(Bank_code1)
            ) COMMENT='存量客户最新风险等级划分'
        """)

        conn.commit()
        print("\n[SUCCESS] 所有表创建成功!")

        # 验证表创建
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        print(f"\n成功创建 {len(tables)} 个表:")
        for table in tables:
            print(f"  - {table[0]}")

        cursor.close()
        conn.close()

        return True

    except Exception as e:
        print(f"[ERROR] 创建表失败: {e}")
        return False

if __name__ == "__main__":
    success = create_aml300_tables()
    sys.exit(0 if success else 1)