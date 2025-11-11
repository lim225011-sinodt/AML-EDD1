#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
最终验证报告：AML300数据库当前状态
按照软件开发标准，对比设计与实际生成的一致性
"""

import mysql.connector
import sys
from datetime import datetime

def final_verification():
    """最终验证报告"""
    print("=" * 60)
    print("AML300数据库最终验证报告")
    print("软件工程标准：设计与实际一致性验证")
    print(f"验证时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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

        # 设计规格
        design_specs = {
            '表结构': {'目标': 15, '描述': '300号文件要求的完整表结构'},
            '个人客户': {'目标': 10, '描述': '个人客户数量'},
            '企业客户': {'目标': 2, '描述': '企业客户数量'},
            '账户': {'目标': 12, '描述': '总账户数量（10个人+2企业）'},
            '风险记录': {'目标': '>=17', '描述': '风险等级记录（最新12+历史≥5）'},
            '交易记录': {'目标': '>=20', '描述': '各类交易记录'},
            '报告记录': {'目标': '>=8', '描述': '大额+可疑交易报告'}
        }

        print("\n第一部分：表结构完整性")
        print("-" * 40)

        # 检查所有表
        cursor.execute("SHOW TABLES")
        all_tables = [table[0] for table in cursor.fetchall()]
        expected_tables = [
            'tb_bank', 'tb_settle_type', 'tb_cst_pers', 'tb_cst_unit', 'tb_acc',
            'tb_acc_txn', 'tb_risk_his', 'tb_risk_new', 'tb_cred_txn',
            'tb_cash_remit', 'tb_cash_convert', 'tb_cross_border',
            'tb_lwhc_log', 'tb_lar_report', 'tb_sus_report'
        ]

        missing_tables = []
        existing_tables = []

        for table in expected_tables:
            if table in all_tables:
                existing_tables.append(table)
                print(f"  ✅ {table}")
            else:
                missing_tables.append(table)
                print(f"  ❌ {table}")

        structure_score = len(existing_tables) / len(expected_tables) * 100
        print(f"\n表结构完整性: {len(existing_tables)}/{len(expected_tables)} ({structure_score:.1f}%)")

        print("\n第二部分：数据量达标情况")
        print("-" * 40)

        data_stats = {}
        total_records = 0

        # 获取每张表的记录数
        table_checks = [
            ('tb_cst_pers', '个人客户', 10),
            ('tb_cst_unit', '企业客户', 2),
            ('tb_acc', '账户', 12),
            ('tb_risk_new', '最新风险等级', 12),
            ('tb_risk_his', '历史风险等级', 5),
            ('tb_acc_txn', '账户交易', 20),
            ('tb_cred_txn', '信用卡交易', 10),
            ('tb_cross_border', '跨境交易', 5),
            ('tb_cash_remit', '现金汇款', 5),
            ('tb_cash_convert', '现钞结售汇', 3),
            ('tb_lwhc_log', '联网核查日志', 10),
            ('tb_lar_report', '大额交易报告', 5),
            ('tb_sus_report', '可疑交易报告', 3),
            ('tb_bank', '银行信息', 5),
            ('tb_settle_type', '业务类型', 10)
        ]

        for table, desc, target in table_checks:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                data_stats[table] = {'actual': count, 'target': target, 'desc': desc}
                total_records += count

                if isinstance(target, int):
                    if count == target:
                        status = "✅ 达标"
                    elif count > target:
                        status = f"✅ 超标 ({count} > {target})"
                    else:
                        status = f"❌ 不足 ({count}/{target})"
                else:  # 带比较符的目标
                    if target.startswith('>='):
                        min_val = int(target[2:])
                        if count >= min_val:
                            status = "✅ 达标"
                        else:
                            status = f"❌ 不足 ({count} < {min_val})"

                print(f"  {desc:12} : {count:6d} (目标: {target}) {status}")

            except Exception as e:
                print(f"  {desc:12} : 查询失败 - {e}")

        print(f"\n总记录数: {total_records:,}")

        print("\n第三部分：数据质量检查")
        print("-" * 40)

        quality_checks = []

        # 检查数据关联性
        try:
            # 检查客户-账户关联
            cursor.execute("""
                SELECT COUNT(*) FROM tb_acc a
                WHERE a.Cst_no NOT IN (
                    SELECT Cst_no FROM tb_cst_pers
                    UNION SELECT Cst_no FROM tb_cst_unit
                )
            """)
            orphan_accounts = cursor.fetchone()[0]
            if orphan_accounts == 0:
                quality_checks.append("✅ 客户-账户关联正常")
            else:
                quality_checks.append(f"❌ 发现{orphan_accounts}个孤立账户")

            # 检查风险-客户关联
            cursor.execute("""
                SELECT COUNT(*) FROM tb_risk_new r
                WHERE r.Cst_no NOT IN (
                    SELECT Cst_no FROM tb_cst_pers
                    UNION SELECT Cst_no FROM tb_cst_unit
                )
            """)
            orphan_risks = cursor.fetchone()[0]
            if orphan_risks == 0:
                quality_checks.append("✅ 风险-客户关联正常")
            else:
                quality_checks.append(f"❌ 发现{orphan_risks}个孤立风险记录")

        except Exception as e:
            quality_checks.append(f"❌ 关联性检查失败: {e}")

        for check in quality_checks:
            print(f"  {check}")

        print("\n第四部分：符合性评估")
        print("-" * 40)

        # 核心指标对比
        print("核心业务指标对比:")
        key_metrics = [
            ('tb_cst_pers', '个人客户'),
            ('tb_cst_unit', '企业客户'),
            ('tb_acc', '账户总数'),
            ('tb_acc_txn + tb_cred_txn', '交易记录'),
            ('tb_lar_report + tb_sus_report', '报告记录')
        ]

        for table_expr, desc in key_metrics:
            if '+' in table_expr:
                # 多表合计
                tables = table_expr.split(' + ')
                total = sum(data_stats.get(t, {}).get('actual', 0) for t in tables)
                target = sum(design_specs.get(t, {}).get('target', 0) for t in tables if t in design_specs)
            else:
                total = data_stats.get(table_expr, {}).get('actual', 0)
                target = design_specs.get(table_expr, {}).get('target', 0)

            if isinstance(target, int) and total >= target:
                status = "✅"
            elif isinstance(target, str) and target.startswith('>=') and total >= int(target[2:]):
                status = "✅"
            else:
                status = "❌"

            print(f"  {desc:10} : 实际{total:6d}, 目标{target:8d} {status}")

        print("\n第五部分：最终评估")
        print("-" * 40)

        # 计算总体得分
        data_score = 0
        max_score = 0

        for table, info in design_specs.items():
            if table == '表结构':
                score = structure_score
            elif table in data_stats:
                actual = data_stats[table]['actual']
                target = info['target']
                if isinstance(target, int):
                    if actual == target:
                        score = 100
                    elif actual > target:
                        score = 100  # 超标也算满分
                    else:
                        score = actual / target * 100
                else:  # 带比较符的目标
                    min_val = int(target[2:])
                    score = 100 if actual >= min_val else (actual / min_val * 100)
            else:
                score = 0

            data_score += score
            max_score += 100

        overall_score = data_score / max_score if max_score > 0 else 0

        print(f"总体得分: {overall_score:.1f}/100")

        # 状态评估
        if overall_score >= 90:
            status = "优秀"
            color = "绿色"
            recommendation = "数据生成质量优秀，可以投入生产使用"
        elif overall_score >= 80:
            status = "良好"
            color = "蓝色"
            recommendation = "数据生成质量良好，建议补充部分缺失数据"
        elif overall_score >= 60:
            status = "一般"
            color = "黄色"
            recommendation = "数据生成质量一般，需要大量补充数据"
        else:
            status = "需要改进"
            color = "红色"
            recommendation = "数据生成质量不足，需要重新生成数据"

        print(f"评估结果: [{status}]")
        print(f"状态颜色: {color}")
        print(f"改进建议: {recommendation}")

        print("\n第六部分：实施建议")
        print("-" * 40)

        print("紧急改进项:")
        if data_stats.get('tb_cst_pers', {}).get('actual', 0) < 10:
            print("  1. 补充个人客户数据，目标：10个")
        if data_stats.get('tb_cst_unit', {}).get('actual', 0) < 2:
            print("  2. 补充企业客户数据，目标：2个")

        txn_total = (data_stats.get('tb_acc_txn', {}).get('actual', 0) +
                   data_stats.get('tb_cred_txn', {}).get('actual', 0))
        if txn_total < 20:
            print("  3. 补充交易记录数据，目标：≥20条")

        report_total = (data_stats.get('tb_lar_report', {}).get('actual', 0) +
                       data_stats.get('tb_sus_report', {}).get('actual', 0))
        if report_total < 8:
            print("  4. 补充交易报告数据，目标：≥8条")

        print("\n实施计划:")
        print("  阶段1: 修复个人和企业客户数据生成程序")
        print("  阶段2: 批量生成各类交易记录")
        print("  阶段3: 生成风险等级和报告数据")
        print("  阶段4: 最终验证和测试")

        print(f"\n结论：")
        print(f"  当前AML300数据库已完成基础表结构建设，")
        print(f"  数据覆盖率需要提升以满足300号文件要求。")
        print(f"  建议立即实施数据补充计划。")

        cursor.close()
        conn.close()

        return overall_score >= 60

    except Exception as e:
        print(f"验证过程中发生错误: {e}")
        return False

if __name__ == "__main__":
    success = final_verification()
    sys.exit(0 if success else 1)