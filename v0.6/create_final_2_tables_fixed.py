#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修复并创建AML300数据库最后2张表（解决主键约束问题）
"""

import mysql.connector
import sys

def create_final_2_tables():
    """修复主键约束问题并创建最后2张表"""
    print("=== 修复并创建AML300数据库最后2张表 ===\n")

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

        # 先删除可能存在的错误表
        cursor.execute("DROP TABLE IF EXISTS tb_lar_report")
        cursor.execute("DROP TABLE IF EXISTS tb_sus_report")
        print("已清理可能存在的错误表")

        # 14) 大额交易报告明细（修复版 - 移除NULL值的主键字段）
        print("创建大额交易报告表（修复版）...")
        cursor.execute("""
            CREATE TABLE tb_lar_report (
                report_id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '报告ID（主键）',
                RLFC   CHAR(2)       NOT NULL COMMENT '金融机构与客户关系：00本行账户/卡；01境外卡收单；02非账户业务机构',
                ROTF   VARCHAR(64)   NULL COMMENT '交易信息备注（如无填@N）',
                RPMN   VARCHAR(500)  NOT NULL COMMENT '收付款方匹配号',
                RPMT   CHAR(2)       NULL COMMENT '收付款方匹配号类型（按监测中心代码表）',
                Report_Date CHAR(8)       NOT NULL COMMENT '报告日期',
                Institution_Name VARCHAR(200) NOT NULL COMMENT '报告机构名称',
                Report_Amount DECIMAL(18,2) NOT NULL COMMENT '报告金额',
                Currency CHAR(3)       NOT NULL COMMENT '币种',
                Transaction_Type VARCHAR(50) NOT NULL COMMENT '交易类型',
                Transaction_Date CHAR(8)       NOT NULL COMMENT '交易日期',
                Customer_Name VARCHAR(200) NOT NULL COMMENT '客户姓名/名称',
                Customer_ID VARCHAR(100) NOT NULL COMMENT '客户标识号',
                Account_No VARCHAR(50) NULL COMMENT '账号/卡号',
                KEY idx_lar_rlfcpmn (RLFC, RPMN),
                KEY idx_lar_date (Report_Date),
                KEY idx_lar_amount (Report_Amount),
                KEY idx_lar_institution (Institution_Name)
            ) COMMENT='大额交易报告明细（监测中心数据字典口径）'
        """)

        # 15) 可疑交易报告明细（修复版 - 确保主键字段NOT NULL）
        print("创建可疑交易报告表（修复版）...")
        cursor.execute("""
            CREATE TABLE tb_sus_report (
                report_id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '报告ID（主键）',
                TBID  VARCHAR(128)  NULL COMMENT '交易代办人证件号码（居民身份证15/18位等口径）',
                TBIT  CHAR(6)       NULL COMMENT '交易代办人证件类型（10.1节代码表）',
                TBNM  VARCHAR(128)  NULL COMMENT '交易代办人姓名',
                TBNT  CHAR(3)       NULL COMMENT '交易代办人国籍（GB/T2659-2000）',
                TCAC  VARCHAR(64)   NULL COMMENT '交易对手账号',
                TCAT  CHAR(6)       NULL COMMENT '交易对手账户类型（10.2节）',
                TCID  VARCHAR(128)  NULL COMMENT '交易对手证件号码（机构9位组织机构代码口径等）',
                TCIT  CHAR(6)       NULL COMMENT '交易对手证件类型（10.1节）',
                TCNM  VARCHAR(128)  NULL COMMENT '交易对手姓名/名称',
                TICD  VARCHAR(256)  NOT NULL COMMENT '业务标识号',
                TRCD  CHAR(9)       NULL COMMENT '交易发生地（前3位国别/CHN，后6位区县或000000）',
                Report_Date CHAR(8)       NOT NULL COMMENT '报告日期',
                Institution_Name VARCHAR(200) NOT NULL COMMENT '报告机构名称',
                Transaction_Amount DECIMAL(18,2) NOT NULL COMMENT '交易金额',
                Currency CHAR(3)       NOT NULL COMMENT '币种',
                Transaction_Type VARCHAR(100) NOT NULL COMMENT '可疑交易类型',
                Suspicious_Reason VARCHAR(500) NOT NULL COMMENT '可疑交易特征/理由',
                Report_Time CHAR(8)       NOT NULL COMMENT '报告时间',
                KEY idx_sus_ticd (TICD),
                KEY idx_sus_date (Report_Date),
                KEY idx_sus_amount (Transaction_Amount),
                KEY idx_sus_institution (Institution_Name),
                UNIQUE KEY uk_sus_ticd (TICD)
            ) COMMENT='可疑交易报告明细（监测中心数据字典口径）'
        """)

        conn.commit()
        print("\n[SUCCESS] 最后2张表创建成功!")

        # 验证总表数
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]

        print(f"\nAML300数据库现在包含 {len(tables)} 张表:")
        for table in tables:
            print(f"  - {table}")

        expected = 15
        if len(tables) == expected:
            print(f"\n🎉 完整的300号文件15张表结构已创建完成!")
            success = True
        else:
            print(f"\n⚠️  期望 {expected} 张表，实际 {len(tables)} 张表")
            success = False

        cursor.close()
        conn.close()

        return success

    except Exception as e:
        print(f"[ERROR] 创建表失败: {e}")
        return False

if __name__ == "__main__":
    success = create_final_2_tables()
    sys.exit(0 if success else 1)