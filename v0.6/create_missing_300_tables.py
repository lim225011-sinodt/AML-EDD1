#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
创建AML300数据库缺失的7张表（补充完整的15张表）
"""

import mysql.connector
import sys

def create_missing_tables():
    """创建AML300数据库缺失的7张表"""
    print("=== 创建AML300数据库缺失的7张表 ===\n")

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

        # 8) 贷记（信用卡/准贷记卡）交易
        print("创建贷记账户交易表...")
        cursor.execute("""
            CREATE TABLE tb_cred_txn (
              Self_acc_no   VARCHAR(40)  NOT NULL COMMENT '账号（信用卡账户）',
              Card_no       VARCHAR(40)  NULL COMMENT '卡号',
              Self_acc_name VARCHAR(120) NOT NULL COMMENT '姓名/名称',
              Cst_no        VARCHAR(50)  NOT NULL COMMENT '客户号',
              Id_no         VARCHAR(50)  NOT NULL COMMENT '证件号（个人表3；单位表4 License）',
              Date          CHAR(8)      NOT NULL COMMENT '交易日期',
              Time          CHAR(6)      NOT NULL COMMENT '交易时间',
              Lend_flag     CHAR(2)      NOT NULL COMMENT '收付：10收；11付（客户角度）',
              Tsf_flag      CHAR(2)      NOT NULL COMMENT '现金/转账：10现金；11转账',
              Cur           CHAR(3)      NOT NULL COMMENT '币种',
              Org_amt       DECIMAL(16,2) NOT NULL COMMENT '原币种交易金额',
              Usd_amt       DECIMAL(16,2) NOT NULL COMMENT '折美元交易金额',
              Rmb_amt       DECIMAL(16,2) NOT NULL COMMENT '折人民币交易金额',
              Balance       DECIMAL(16,2) NULL COMMENT '溢缴款余额（若有）',
              Purpose       VARCHAR(120) NULL COMMENT '摘要/用途',
              Pos_owner     VARCHAR(40)  NULL COMMENT '消费商户名称',
              Trans_type    CHAR(2)      NULL COMMENT '交易类型：11有卡；12网银；13非网银无卡',
              Ip_code       VARCHAR(17)  NULL COMMENT '网银IP（非网银无卡时）',
              Bord_flag     CHAR(2)      NULL COMMENT '跨境标识：11是；12否',
              Nation        VARCHAR(20)  NULL COMMENT '对方国家/地区（跨境=11时）',
              PRIMARY KEY (Self_acc_no, Date, Time, Lend_flag),
              KEY idx_cred_combo1 (Date, Self_acc_no, Lend_flag),
              KEY idx_cred_combo2 (Date, Id_no, Lend_flag),
              KEY idx_cred_combo3 (Date, Id_no, Self_acc_no)
            ) COMMENT='信用卡/准贷记卡等贷记账户交易'
        """)

        # 9) 现金汇款/无卡无折现金存款（非跨境）
        print("创建现金汇款表...")
        cursor.execute("""
            CREATE TABLE tb_cash_remit (
              Date          CHAR(8)      NOT NULL COMMENT '交易日期',
              Time          CHAR(6)      NOT NULL COMMENT '交易时间',
              Self_bank_code VARCHAR(20) NOT NULL COMMENT '交易行代码=tb_bank.Bank_code1',
              Acc_name      VARCHAR(60)  NOT NULL COMMENT '客户姓名/名称（办理现金业务的客户）',
              Id_no         VARCHAR(50)  NOT NULL COMMENT '证件号（个人表3；单位表4 License）',
              Cur           CHAR(3)      NOT NULL COMMENT '币种',
              Org_amt       DECIMAL(16,2) NOT NULL COMMENT '原币种金额',
              Usd_amt       DECIMAL(16,2) NOT NULL COMMENT '折美元金额',
              Rmb_amt       DECIMAL(16,2) NOT NULL COMMENT '折人民币金额',
              Part_bank     VARCHAR(120) NULL COMMENT '交易对手银行名称',
              Part_acc_no   VARCHAR(40)  NULL COMMENT '交易对方账号/卡号',
              Part_acc_name VARCHAR(120) NULL COMMENT '交易对方账户名称',
              Settle_type   VARCHAR(20)  NULL COMMENT '业务类型（指向tb_settle_type）',
              Ticd          VARCHAR(40)  NULL COMMENT '业务流水号',
              PRIMARY KEY (Date, Time, Self_bank_code, Id_no),
              KEY idx_cr_combo1 (Date, Cur, Id_no),
              KEY idx_cr_combo2 (Date, Id_no, Part_acc_no),
              CONSTRAINT fk_cr_bank FOREIGN KEY (Self_bank_code) REFERENCES tb_bank(Bank_code1),
              CONSTRAINT fk_cr_settle FOREIGN KEY (Settle_type) REFERENCES tb_settle_type(Settle_type)
            ) COMMENT='现金汇款/无卡无折现金存款（非跨境）'
        """)

        # 10) 现钞结售汇/兑换明细（账户外）
        print("创建现钞结售汇表...")
        cursor.execute("""
            CREATE TABLE tb_cash_convert (
              Date        CHAR(8)      NOT NULL COMMENT '交易日期',
              Time        CHAR(6)      NOT NULL COMMENT '交易时间',
              Self_bank_code VARCHAR(20) NOT NULL COMMENT '交易行代码=tb_bank.Bank_code1',
              Acc_name    VARCHAR(60)  NOT NULL COMMENT '客户姓名/名称',
              Id_no       VARCHAR(50)  NOT NULL COMMENT '证件号（个人表3；单位表4 License）',
              Out_cur     CHAR(3)      NOT NULL COMMENT '付出币种（国标）',
              Out_org_amt DECIMAL(16,2) NOT NULL COMMENT '付出原币金额',
              Out_usd_amt DECIMAL(16,2) NOT NULL COMMENT '付出折美元',
              In_cur      CHAR(3)      NOT NULL COMMENT '收入币种（国标）',
              In_org_amt  DECIMAL(16,2) NOT NULL COMMENT '收入原币金额',
              In_usd_amt  DECIMAL(16,2) NOT NULL COMMENT '收入折美元',
              Ticd        VARCHAR(40)  NULL COMMENT '业务流水号',
              Counter_no  VARCHAR(30)  NULL COMMENT '柜员号',
              Settle_type VARCHAR(20)  NULL COMMENT '业务类型（指向tb_settle_type）',
              PRIMARY KEY (Date, Time, Self_bank_code, Id_no),
              KEY idx_cc_combo1 (Date, Id_no),
              KEY idx_cc_combo2 (Date, Out_cur, Id_no),
              CONSTRAINT fk_cc_bank FOREIGN KEY (Self_bank_code) REFERENCES tb_bank(Bank_code1),
              CONSTRAINT fk_cc_settle FOREIGN KEY (Settle_type) REFERENCES tb_settle_type(Settle_type)
            ) COMMENT='现钞结售汇/外币现钞兑换（账户外流动）'
        """)

        # 11) 跨境汇款交易
        print("创建跨境汇款交易表...")
        cursor.execute("""
            CREATE TABLE tb_cross_border (
              Date          CHAR(8)      NOT NULL COMMENT '交易日期',
              Time          CHAR(6)      NOT NULL COMMENT '交易时间',
              Lend_flag     CHAR(2)      NOT NULL COMMENT '收付标识：10收；11付（客户角度）',
              Tsf_flag      CHAR(2)      NOT NULL COMMENT '现金/转账标识：10现金；11转账',
              Unit_flag     CHAR(2)      NOT NULL COMMENT '公私标识：11个人；12单位（客户）',
              Cst_no        VARCHAR(50)  NOT NULL COMMENT '客户号',
              Id_no         VARCHAR(50)  NOT NULL COMMENT '证件号（个人按表3；单位按表4 License）',
              Self_acc_name VARCHAR(120) NOT NULL COMMENT '客户姓名/名称',
              Self_acc_no   VARCHAR(60)  NOT NULL COMMENT '客户账号',
              Card_no       VARCHAR(60)  NULL COMMENT '客户卡号',
              Self_add      VARCHAR(120) NULL COMMENT '客户联系地址',
              Ticd          VARCHAR(40)  NULL COMMENT '业务流水号',
              Part_acc_no   VARCHAR(60)  NULL COMMENT '交易对手账号',
              Part_acc_name VARCHAR(120) NULL COMMENT '交易对手姓名/名称',
              Part_nation   VARCHAR(20)  NULL COMMENT '交易对手国家/地区（GB/T2659-2000）',
              Cur           CHAR(3)      NOT NULL COMMENT '币种（国标）',
              Org_amt       DECIMAL(16,2) NOT NULL COMMENT '原币种金额',
              Usd_amt       DECIMAL(16,2) NOT NULL COMMENT '折美元金额',
              Rmb_amt       DECIMAL(16,2) NOT NULL COMMENT '折人民币金额',
              Balance       DECIMAL(16,2) NULL COMMENT '交易后原币种余额',
              Agency_flag   CHAR(2)      NULL COMMENT '是否代理：11代理；12本人',
              Agent_name    VARCHAR(60)  NULL,
              Agent_tel     VARCHAR(60)  NULL,
              Agent_type    CHAR(2)      NULL,
              Agent_no      VARCHAR(50)  NULL,
              Settle_type   VARCHAR(20)  NULL COMMENT '业务类型编码（指向tb_settle_type）',
              Reverse_flag  CHAR(2)      NOT NULL COMMENT '冲账标识：10否；11是',
              Purpose       VARCHAR(120) NULL COMMENT '摘要/用途',
              Bord_flag     CHAR(2)      NOT NULL COMMENT '跨境标识：11是；12否',
              Nation        VARCHAR(20)  NULL COMMENT '对方所在国家/地区（跨境=11时填）',
              Bank_flag     CHAR(2)      NULL COMMENT '交易方式：11网银；12手机；13柜面；14ATM；15其他',
              Ip_code       VARCHAR(17)  NULL COMMENT '网银IP',
              Atm_code      VARCHAR(30)  NULL COMMENT 'ATM机具编号',
              Bank_code     VARCHAR(20)  NULL COMMENT 'ATM机具所属行号=tb_bank.Bank_code1',
              Mac_info      VARCHAR(17)  NULL COMMENT 'MAC/IMEI',
              PRIMARY KEY (Date, Time, Self_acc_no, Lend_flag),
              KEY idx_cb_id (Date, Id_no),
              KEY idx_cb_acc (Date, Self_acc_no),
              CONSTRAINT fk_cb_settle FOREIGN KEY (Settle_type) REFERENCES tb_settle_type(Settle_type)
            ) COMMENT='跨境汇款交易数据表'
        """)

        # 13) 公民联网核查日志
        print("创建公民联网核查日志表...")
        cursor.execute("""
            CREATE TABLE tb_lwhc_log (
              Bank_name  VARCHAR(120) NOT NULL COMMENT '核查机构名称=tb_bank.Bank_name',
              Bank_code2 VARCHAR(20)  NOT NULL COMMENT '核查机构网点代码=tb_bank.Bank_code1',
              Date       CHAR(8)      NOT NULL COMMENT '联网核查日期',
              Time       CHAR(6)      NOT NULL COMMENT '联网核查时间',
              Name       VARCHAR(40)  NOT NULL COMMENT '公民姓名',
              Id_no      VARCHAR(50)  NOT NULL COMMENT '居民身份证号码',
              Result     CHAR(2)      NOT NULL COMMENT '核查结果（11一致有照…16其他）',
              Counter_no VARCHAR(30)  NULL COMMENT '柜员号',
              Ope_line   VARCHAR(40)  NULL COMMENT '业务条线/岗位',
              Mode       CHAR(2)      NULL COMMENT '核查方式：10机读；11手动',
              Purpose    VARCHAR(120) NULL COMMENT '摘要/备注',
              PRIMARY KEY (Date, Time, Id_no),
              KEY idx_lwhc_combo1 (Id_no, Date),
              KEY idx_lwhc_combo2 (Id_no, Result)
            ) COMMENT='公民联网核查日志记录'
        """)

        # 14) 大额交易报告明细（已上报成功口径）
        print("创建大额交易报告表...")
        cursor.execute("""
            CREATE TABLE tb_lar_report (
              RLFC   CHAR(2)       NOT NULL COMMENT '金融机构与客户关系：00本行账户/卡；01境外卡收单；02非账户业务机构',
              ROTF   VARCHAR(64)   NULL COMMENT '交易信息备注（如无填@N）',
              RPMN   VARCHAR(500)  NULL COMMENT '收付款方匹配号',
              RPMT   CHAR(2)       NULL COMMENT '收付款方匹配号类型（按监测中心代码表）',
              Report_Date CHAR(8)       NULL COMMENT '报告日期',
              Institution_Name VARCHAR(200) NULL COMMENT '报告机构名称',
              Report_Amount DECIMAL(18,2) NULL COMMENT '报告金额',
              Currency CHAR(3)       NULL COMMENT '币种',
              Transaction_Type VARCHAR(50) NULL COMMENT '交易类型',
              Transaction_Date CHAR(8)       NULL COMMENT '交易日期',
              Customer_Name VARCHAR(200) NULL COMMENT '客户姓名/名称',
              Customer_ID VARCHAR(100) NULL COMMENT '客户标识号',
              Account_No VARCHAR(50) NULL COMMENT '账号/卡号',
              PRIMARY KEY (RLFC, RPMN),
              KEY idx_lar_date (Report_Date),
              KEY idx_lar_amount (Report_Amount)
            ) COMMENT='大额交易报告明细（监测中心数据字典口径）'
        """)

        # 15) 可疑交易报告明细（已上报成功口径）
        print("创建可疑交易报告表...")
        cursor.execute("""
            CREATE TABLE tb_sus_report (
              TBID  VARCHAR(128)  NULL COMMENT '交易代办人证件号码（居民身份证15/18位等口径）',
              TBIT  CHAR(6)       NULL COMMENT '交易代办人证件类型（10.1节代码表）',
              TBNM  VARCHAR(128)  NULL COMMENT '交易代办人姓名',
              TBNT  CHAR(3)       NULL COMMENT '交易代办人国籍（GB/T2659-2000）',
              TCAC  VARCHAR(64)   NULL COMMENT '交易对手账号',
              TCAT  CHAR(6)       NULL COMMENT '交易对手账户类型（10.2节）',
              TCID  VARCHAR(128)  NULL COMMENT '交易对手证件号码（机构9位组织机构代码口径等）',
              TCIT  CHAR(6)       NULL COMMENT '交易对手证件类型（10.1节）',
              TCNM  VARCHAR(128)  NULL COMMENT '交易对手姓名/名称',
              TICD  VARCHAR(256)  NULL COMMENT '业务标识号',
              TRCD  CHAR(9)       NULL COMMENT '交易发生地（前3位国别/CHN，后6位区县或000000）',
              Report_Date CHAR(8)       NULL COMMENT '报告日期',
              Institution_Name VARCHAR(200) NULL COMMENT '报告机构名称',
              Transaction_Amount DECIMAL(18,2) NULL COMMENT '交易金额',
              Currency CHAR(3)       NULL COMMENT '币种',
              Transaction_Type VARCHAR(100) NULL COMMENT '可疑交易类型',
              Suspicious_Reason VARCHAR(500) NULL COMMENT '可疑交易特征/理由',
              Report_Time CHAR(8)       NULL COMMENT '报告时间',
              PRIMARY KEY (TICD),
              KEY idx_sus_date (Report_Date),
              KEY idx_sus_amount (Transaction_Amount),
              KEY idx_sus_institution (Institution_Name)
            ) COMMENT='可疑交易报告明细（监测中心数据字典口径）'
        """)

        conn.commit()
        print("\n[SUCCESS] 缺失的7张表创建成功!")

        # 验证表创建
        cursor.execute("SHOW TABLES")
        all_tables = cursor.fetchall()
        print(f"\nAML300数据库现在包含 {len(all_tables)} 张表:")
        for table in all_tables:
            print(f"  - {table[0]}")

        # 确认总表数
        expected_tables = 15
        if len(all_tables) == expected_tables:
            print(f"\n✅ 完整的300号文件15张表结构已创建!")
        else:
            print(f"\n⚠️  期望 {expected_tables} 张表，实际 {len(all_tables)} 张表")

        cursor.close()
        conn.close()

        return True

    except Exception as e:
        print(f"[ERROR] 创建表失败: {e}")
        return False

if __name__ == "__main__":
    success = create_missing_tables()
    sys.exit(0 if success else 1)