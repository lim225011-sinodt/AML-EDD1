#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化数据验证：检查数据生成与计划的一致性
测试人员视角
"""

import mysql.connector
import sys

def validate_data():
    """验证数据一致性和完整性"""
    print("=== AML300数据库数据验证报告 ===")
    print("测试人员：检查数据生成与计划一致性")
    print("时间：2025-11-09")
    print("=" * 50)

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

        # 目标数据规格
        targets = {
            'tb_cst_pers': {'name': '个人客户', 'target': 10},
            'tb_cst_unit': {'name': '企业客户', 'target': 2},
            'tb_acc': {'name': '账户', 'target': 12},
            'tb_risk_new': {'name': '最新风险等级', 'target': 12},
            'tb_risk_his': {'name': '历史风险等级', 'target': '>=5'},
            'tb_acc_txn': {'name': '账户交易', 'target': '>=20'},
            'tb_cred_txn': {'name': '信用卡交易', 'target': '>=10'},
            'tb_cross_border': {'name': '跨境交易', 'target': '>=5'},
            'tb_cash_remit': {'name': '现金汇款', 'target': '>=5'},
            'tb_cash_convert': {'name': '现钞结售汇', 'target': '>=3'},
            'tb_lwhc_log': {'name': '联网核查日志', 'target': '>=10'},
            'tb_lar_report': {'name': '大额交易报告', 'target': '>=5'},
            'tb_sus_report': {'name': '可疑交易报告', 'target': '>=3'}
        }

        print("\n[第一部分] 数据量验证")
        print("-" * 40)

        total_records = 0
        passed_checks = 0
        total_checks = len(targets)

        for table, info in targets.items():
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                actual = cursor.fetchone()[0]
                target = info['target']
                name = info['name']

                total_records += actual

                # 判断是否达标
                if isinstance(target, int):
                    passed = actual == target
                    status = "PASS" if passed else "FAIL"
                    if passed:
                        passed_checks += 1
                    result = f"{actual} (目标: {target})"
                else:  # 带比较符的目标
                    if target.startswith('>='):
                        min_val = int(target[2:])
                        passed = actual >= min_val
                        status = "PASS" if passed else "FAIL"
                        if passed:
                            passed_checks += 1
                        result = f"{actual} (目标: >={min_val})"

                print(f"{name:15} : {result:12} [{status}]")

            except Exception as e:
                print(f"{name:15} : 查询失败 - {e}")

        print(f"\n数据量达标率: {passed_checks}/{total_checks} ({passed_checks/total_checks*100:.1f}%)")
        print(f"总记录数: {total_records:,}")

        print("\n[第二部分] 数据质量检查")
        print("-" * 40)

        # 1. 关联性检查
        print("1. 数据关联性:")
        try:
            # 检查孤立账户
            cursor.execute("""
                SELECT COUNT(*) FROM tb_acc a
                WHERE a.Cst_no NOT IN (
                    SELECT Cst_no FROM tb_cst_pers
                    UNION SELECT Cst_no FROM tb_cst_unit
                )
            """)
            orphan_accounts = cursor.fetchone()[0]
            if orphan_accounts == 0:
                print("   账户-客户关联: [PASS]")
            else:
                print(f"   账户-客户关联: [FAIL] - {orphan_accounts}个孤立账户")

            # 检查孤立风险记录
            cursor.execute("""
                SELECT COUNT(*) FROM tb_risk_new r
                WHERE r.Cst_no NOT IN (
                    SELECT Cst_no FROM tb_cst_pers
                    UNION SELECT Cst_no FROM tb_cst_unit
                )
            """)
            orphan_risks = cursor.fetchone()[0]
            if orphan_risks == 0:
                print("   风险-客户关联: [PASS]")
            else:
                print(f"   风险-客户关联: [FAIL] - {orphan_risks}个孤立风险记录")

        except Exception as e:
            print(f"   关联性检查失败: {e}")

        # 2. 数据完整性
        print("\n2. 数据完整性:")
        try:
            # 检查必填字段
            cursor.execute("SELECT COUNT(*) FROM tb_cst_pers WHERE Id_no IS NULL OR Id_no = ''")
            null_ids = cursor.fetchone()[0]
            if null_ids == 0:
                print("   客户身份证号: [PASS]")
            else:
                print(f"   客户身份证号: [FAIL] - {null_ids}个空值")

            cursor.execute("SELECT COUNT(*) FROM tb_acc WHERE Self_acc_no IS NULL OR Self_acc_no = ''")
            null_accounts = cursor.fetchone()[0]
            if null_accounts == 0:
                print("   账户号码: [PASS]")
            else:
                print(f"   账户号码: [FAIL] - {null_accounts}个空值")

        except Exception as e:
            print(f"   完整性检查失败: {e}")

        # 3. 业务逻辑检查
        print("\n3. 业务逻辑:")
        try:
            # 检查交易金额合理性
            cursor.execute("SELECT COUNT(*) FROM tb_acc_txn WHERE Org_amt < 0")
            negative_amounts = cursor.fetchone()[0]
            if negative_amounts == 0:
                print("   交易金额(非负): [PASS]")
            else:
                print(f"   交易金额(非负): [FAIL] - {negative_amounts}个负金额")

            # 检查日期格式
            cursor.execute("SELECT COUNT(*) FROM tb_acc_txn WHERE Date IS NULL OR LENGTH(Date) != 8")
            invalid_dates = cursor.fetchone()[0]
            if invalid_dates == 0:
                print("   交易日期格式: [PASS]")
            else:
                print(f"   交易日期格式: [FAIL] - {invalid_dates}个无效日期")

        except Exception as e:
            print(f"   业务逻辑检查失败: {e}")

        print("\n[第三部分] 核心业务指标")
        print("-" * 40)

        try:
            # 客户结构分析
            cursor.execute("SELECT COUNT(*) FROM tb_cst_pers")
            pers_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM tb_cst_unit")
            unit_count = cursor.fetchone()[0]

            print(f"客户结构:")
            print(f"  个人客户: {pers_count} 个")
            print(f"  企业客户: {unit_count} 个")
            print(f"  总客户数: {pers_count + unit_count} 个")

            # 账户覆盖度
            cursor.execute("SELECT COUNT(*) FROM tb_acc")
            acc_count = cursor.fetchone()[0]
            expected_accounts = pers_count + unit_count
            coverage = (acc_count / expected_accounts * 100) if expected_accounts > 0 else 0
            print(f"\n账户覆盖度: {coverage:.1f}% ({acc_count}/{expected_accounts})")

            # 交易活跃度
            cursor.execute("SELECT COUNT(*) FROM tb_acc_txn")
            txn_count = cursor.fetchone()[0]
            print(f"交易记录: {txn_count} 条")

            if acc_count > 0:
                avg_txn_per_acc = txn_count / acc_count
                print(f"平均交易/账户: {avg_txn_per_acc:.1f} 条")

        except Exception as e:
            print(f"业务指标检查失败: {e}")

        print("\n[第四部分] 验证结论")
        print("-" * 40)

        # 计算整体评分
        score = passed_checks / total_checks * 100

        print(f"整体评分: {score:.1f}/100")

        if score >= 90:
            print("结论: [优秀] - 数据生成完全符合计划要求")
        elif score >= 80:
            print("结论: [良好] - 数据生成基本符合计划要求")
        elif score >= 70:
            print("结论: [一般] - 数据生成部分符合计划要求")
        else:
            print("结论: [需要改进] - 数据生成与计划要求差距较大")

        print("\n改进建议:")
        if pers_count < 10:
            print("  - 个人客户数量未达到目标10个")
        if unit_count < 2:
            print("  - 企业客户数量未达到目标2个")
        if acc_count < 11:
            print("  - 建议增加账户数量以提升测试覆盖率")
        if txn_count < 20:
            print("  - 建议增加交易记录数量以测试更多业务场景")

        cursor.close()
        conn.close()

        return score >= 80

    except Exception as e:
        print(f"验证过程发生错误: {e}")
        return False

if __name__ == "__main__":
    success = validate_data()
    sys.exit(0 if success else 1)