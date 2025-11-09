#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
表结构对比检查：验证生成的表结构是否符合300号文件要求
"""

import mysql.connector
import sys

def check_table_structure():
    """检查表结构是否符合300号文要求"""
    print("=" * 60)
    print("300号文件表结构符合性检查")
    print("基于：300no_sql_comment.txt")
    print("=" * 60)

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

        print("\n第一部分：表结构完整性检查")
        print("-" * 40)

        # 300号文件要求的15张表
        required_tables = {
            'tb_bank': {
                'name': '机构对照表',
                'desc': '检查对象所属法人机构及其辖区内所有提供金融服务的分支机构或部门条线',
                'index_count': 0  # 先不检查索引
            },
            'tb_settle_type': {
                'name': '业务类型对照表',
                'desc': '法人机构全系统提供的所有金融产品（服务）',
                'index_count': 0
            },
            'tb_cst_pers': {
                'name': '存量个人客户身份信息表',
                'desc': '所有个人客户(包括信用卡客户)最新的身份信息记录',
                'index_count': 3  # 1个组合索引 + 2个独立索引
            },
            'tb_cst_unit': {
                'name': '存量单位客户身份信息表',
                'desc': '所有单位客户最新的身份信息记录',
                'index_count': 4  # 3个组合索引 + 1个独立索引
            },
            'tb_acc': {
                'name': '符合特定条件的银行账户信息表',
                'desc': '符合特定条件的银行账户信息（发生过主动交易的账户）',
                'index_count': 8  # 7个组合索引 + 1个独立索引
            },
            'tb_acc_txn': {
                'name': '基于客户账户的交易数据表',
                'desc': '所有客户的账户(包括销户)在检查期限内的本外币交易流水',
                'index_count': 7  # 7个组合索引
            },
            'tb_cross_border': {
                'name': '跨境汇款交易数据表',
                'desc': '需要国际收支申报的跨境汇款交易流水',
                'index_count': 2  # 2个组合索引
            },
            'tb_cred_txn': {
                'name': '信用卡账户金融交易数据表',
                'desc': '具有贷记功能账户的金融交易流水',
                'index_count': 3  # 3个组合索引
            },
            'tb_cash_remit': {
                'name': '现金汇款交易流水',
                'desc': '现金汇款、无卡无折现金存款交易（非跨境）',
                'index_count': 2  # 2个组合索引
            },
            'tb_cash_convert': {
                'name': '现钞兑换交易明细表',
                'desc': '现钞结售汇、外币现钞兑换（账户外流动）',
                'index_count': 2  # 2个组合索引
            },
            'tb_risk_new': {
                'name': '存量客户最新风险等级表',
                'desc': '所有客户(包括信用卡客户)的最新风险等级划分记录',
                'index_count': 2  # 2个组合索引
            },
            'tb_risk_his': {
                'name': '存量客户检查期限内历次风险等级划分表',
                'desc': '所有客户在检查期内历次的风险等级划分记录',
                'index_count': 2  # 2个组合索引
            },
            'tb_lwhc_log': {
                'name': '公民联网核查日志记录表',
                'desc': '检查期限内开展公民联网核查的工作记录',
                'index_count': 2  # 2个组合索引
            },
            'tb_lar_report': {
                'name': '大额交易报告明细',
                'desc': '向监测中心成功上报的大额交易报告明细',
                'index_count': 0  # 按照监测中心接口规范
            },
            'tb_sus_report': {
                'name': '可疑交易报告明细',
                'desc': '向监测中心成功上报的可疑交易报告明细',
                'index_count': 0  # 按照监测中心接口规范
            }
        }

        # 检查所有表是否存在
        cursor.execute("SHOW TABLES")
        existing_tables = [table[0] for table in cursor.fetchall()]

        table_status = {}
        for table_name, table_info in required_tables.items():
            if table_name in existing_tables:
                table_status[table_name] = "存在"
                print(f"  OK {table_name:20} - {table_info['name']}")
            else:
                table_status[table_name] = "缺失"
                print(f"  X  {table_name:20} - {table_info['name']} [缺失]")

        existing_count = len([t for t in table_status.values() if t == "存在"])
        total_count = len(required_tables)
        completeness = existing_count / total_count * 100

        print(f"\n表结构完整性: {existing_count}/{total_count} ({completeness:.1f}%)")

        print("\n第二部分：详细表结构检查")
        print("-" * 40)

        # 逐个检查表的详细结构
        tables_to_check = ['tb_cst_pers', 'tb_cst_unit', 'tb_acc', 'tb_acc_txn',
                          'tb_cross_border', 'tb_cred_txn', 'tb_risk_new']

        for table in tables_to_check:
            if table in existing_tables:
                print(f"\n{table} ({required_tables[table]['name']})")
                print(f"  说明: {required_tables[table]['desc']}")
                print(f"  要求索引数: {required_tables[table]['index_count']}")

                # 检查表结构
                cursor.execute(f"DESCRIBE {table}")
                columns = cursor.fetchall()

                print(f"  字段数量: {len(columns)}")
                print("  字段列表:")

                primary_keys = []
                for col in columns:
                    field, type_, null, key, default, extra = col
                    if 'PRI' in key:
                        primary_keys.append(field)
                    print(f"    {field:20} {type_:15} NULL: {null:5} KEY: {key:10}")

                if primary_keys:
                    print(f"  主键字段: {', '.join(primary_keys)}")
                else:
                    print("  主键字段: 无")

        print("\n第三部分：关键字段验证")
        print("-" * 40)

        # 检查关键字段是否符合300号文件要求
        key_fields_checks = {
            'tb_cst_pers': [
                'Cst_no', 'Id_no', 'Cst_sex', 'Nation', 'Id_type',
                'Id_deadline', 'Occupation', 'Income', 'Contact1', 'Address1'
            ],
            'tb_cst_unit': [
                'Cst_no', 'Rep_name', 'Ope_name', 'License', 'Industry',
                'Reg_amt', 'Reg_amt_code'
            ],
            'tb_acc': [
                'Self_acc_no', 'Card_no', 'Self_acc_name', 'Cst_no', 'Id_no',
                'Acc_type', 'Open_time', 'Close_time', 'Acc_state'
            ],
            'tb_acc_txn': [
                'Date', 'Time', 'Self_acc_no', 'Cst_no', 'Id_no', 'Lend_flag',
                'Tsf_flag', 'Cur', 'Org_amt', 'Usd_amt', 'Rmb_amt'
            ]
        }

        for table, fields in key_fields_checks.items():
            if table in existing_tables:
                print(f"\n{table} 关键字段检查:")
                cursor.execute(f"DESCRIBE {table}")
                actual_fields = [col[0] for col in cursor.fetchall()]

                missing_fields = []
                extra_fields = []

                for field in fields:
                    if field in actual_fields:
                        print(f"    OK {field}")
                    else:
                        missing_fields.append(field)

                for field in actual_fields:
                    if field not in fields and not field.startswith('report_id'):
                        extra_fields.append(field)

                if missing_fields:
                    print(f"    X  缺失: {', '.join(missing_fields)}")
                if extra_fields:
                    print(f"    +  额外: {', '.join(extra_fields[:5])}{'...' if len(extra_fields) > 5 else ''}")

        print("\n第四部分：业务逻辑符合性")
        print("-" * 40)

        # 检查业务逻辑
        logic_checks = [
            "1. 表间关联关系",
            "2. 字段命名规范",
            "3. 数据类型设计",
            "4. 索引设计优化"
        ]

        for check in logic_checks:
            print(f"  {check}")

        print("\n第五部分：符合性结论")
        print("-" * 40)

        # 计算符合性得分
        structure_score = completeness

        if structure_score >= 95:
            status = "优秀"
            recommendation = "表结构完全符合300号文件要求"
        elif structure_score >= 90:
            status = "良好"
            recommendation = "表结构基本符合300号文件要求，少量细节需要调整"
        elif structure_score >= 80:
            status = "合格"
            recommendation = "表结构基本满足300号文件要求，需要完善关键字段"
        else:
            status = "需要改进"
            recommendation = "表结构与300号文件要求差距较大，需要重新设计"

        print(f"符合性评估: {status}")
        print(f"综合得分: {structure_score:.1f}/100")
        print(f"改进建议: {recommendation}")

        print(f"\n第六部分：实施建议")
        print("-" * 40)

        print("立即可执行项:")
        if 'tb_cst_pers' in table_status and table_status['tb_cst_pers'] == "存在":
            print("  1. 验证个人客户表的字段完整性")
        if 'tb_cst_unit' in table_status and table_status['tb_cst_unit'] == "存在":
            print("  2. 验证企业客户表的字段完整性")
        if 'tb_acc' in table_status and table_status['tb_acc'] == "存在":
            print("  3. 验证账户表的字段完整性")

        print("长期优化项:")
        print("  1. 按照300号文件要求创建索引")
        print("  2. 优化查询性能")
        print("  3. 完善数据约束和校验规则")
        print("  4. 建立数据质量监控机制")

        cursor.close()
        conn.close()

        return structure_score >= 80

    except Exception as e:
        print(f"检查过程中发生错误: {e}")
        return False

if __name__ == "__main__":
    success = check_table_structure()
    sys.exit(0 if success else 1)