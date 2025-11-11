#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据验证报告：验证AML300数据库中生成的数据是否与计划一致
测试人员视角：数据完整性、逻辑性、业务规范符合性检查
"""

import mysql.connector
import sys
from datetime import datetime

class DataValidationReport:
    """数据验证报告生成器"""

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

        # 预期目标数据量
        self.expected_targets = {
            '个人客户': {'min': 10, 'max': 10, 'name': 'tb_cst_pers'},
            '企业客户': {'min': 2, 'max': 2, 'name': 'tb_cst_unit'},
            '账户': {'min': 11, 'max': 15, 'name': 'tb_acc'},
            '最新风险等级': {'min': 11, 'max': 15, 'name': 'tb_risk_new'},
            '历史风险等级': {'min': 5, 'max': 20, 'name': 'tb_risk_his'},
            '账户交易': {'min': 20, 'max': 100, 'name': 'tb_acc_txn'},
            '信用卡交易': {'min': 10, 'max': 50, 'name': 'tb_cred_txn'},
            '跨境交易': {'min': 5, 'max': 30, 'name': 'tb_cross_border'},
            '现金汇款': {'min': 5, 'max': 20, 'name': 'tb_cash_remit'},
            '现钞结售汇': {'min': 3, 'max': 15, 'name': 'tb_cash_convert'},
            '联网核查日志': {'min': 10, 'max': 50, 'name': 'tb_lwhc_log'},
            '大额交易报告': {'min': 5, 'max': 20, 'name': 'tb_lar_report'},
            '可疑交易报告': {'min': 3, 'max': 15, 'name': 'tb_sus_report'}
        }

        print("=== AML300数据库验证报告 ===")
        print("测试人员视角：数据完整性、逻辑性、业务规范检查")
        print(f"验证时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

    def check_table_data_volume(self):
        """检查各表数据量是否达标"""
        print("\n[数据量] 一、数据量完整性检查")
        print("-" * 40)

        results = {}
        passed = 0
        failed = 0

        for desc, target in self.expected_targets.items():
            try:
                self.cursor.execute(f"SELECT COUNT(*) FROM {target['name']}")
                actual_count = self.cursor.fetchone()[0]

                min_target = target['min']
                max_target = target['max']

                if min_target <= actual_count <= max_target:
                    status = "✅ PASS"
                    passed += 1
                else:
                    status = "❌ FAIL"
                    failed += 1

                results[desc] = {
                    'actual': actual_count,
                    'target': f"{min_target}-{max_target}",
                    'status': status
                }

                print(f"{desc:15} : {actual_count:6d} 条 (目标: {min_target}-{max_target}) {status}")

            except Exception as e:
                print(f"{desc:15} : 查询失败 - {e}")
                failed += 1

        print(f"\n数据量检查结果: {passed} 项通过, {failed} 项失败")
        return results

    def check_data_relationships(self):
        """检查数据关联性"""
        print("\n🔗 二、数据关联性检查")
        print("-" * 40)

        relationship_tests = []

        # 1. 检查客户-账户关联
        try:
            self.cursor.execute("""
                SELECT COUNT(*) FROM tb_acc a
                WHERE a.Cst_no NOT IN (
                    SELECT Cst_no FROM tb_cst_pers
                    UNION SELECT Cst_no FROM tb_cst_unit
                )
            """)
            orphan_accounts = self.cursor.fetchone()[0]
            status = "✅ PASS" if orphan_accounts == 0 else f"❌ FAIL ({orphan_accounts}个孤立账户)"
            relationship_tests.append(("账户-客户关联", status))
            print(f"账户-客户关联     : {status}")

        except Exception as e:
            print(f"账户-客户关联检查失败: {e}")

        # 2. 检查风险等级-客户关联
        try:
            self.cursor.execute("""
                SELECT COUNT(*) FROM tb_risk_new r
                WHERE r.Cst_no NOT IN (
                    SELECT Cst_no FROM tb_cst_pers
                    UNION SELECT Cst_no FROM tb_cst_unit
                )
            """)
            orphan_risks = self.cursor.fetchone()[0]
            status = "✅ PASS" if orphan_risks == 0 else f"❌ FAIL ({orphan_risks}个孤立风险记录)"
            relationship_tests.append(("风险等级-客户关联", status))
            print(f"风险等级-客户关联 : {status}")

        except Exception as e:
            print(f"风险等级-客户关联检查失败: {e}")

        # 3. 检查外键约束
        try:
            # 检查账户交易中的客户是否存在
            self.cursor.execute("""
                SELECT COUNT(*) FROM tb_acc_txn t
                WHERE t.Cst_no NOT IN (
                    SELECT Cst_no FROM tb_cst_pers
                    UNION SELECT Cst_no FROM tb_cst_unit
                )
            """)
            orphan_txns = self.cursor.fetchone()[0]
            status = "✅ PASS" if orphan_txns == 0 else f"❌ FAIL ({orphan_txns}个孤立交易记录)"
            relationship_tests.append(("交易记录-客户关联", status))
            print(f"交易记录-客户关联 : {status}")

        except Exception as e:
            print(f"交易记录-客户关联检查失败: {e}")

        return relationship_tests

    def check_data_quality(self):
        """检查数据质量"""
        print("\n🔍 三、数据质量检查")
        print("-" * 40)

        quality_issues = []

        # 1. 检查必填字段
        try:
            # 检查个人客户身份证号
            self.cursor.execute("""
                SELECT COUNT(*) FROM tb_cst_pers
                WHERE Id_no IS NULL OR Id_no = ''
            """)
            null_ids = self.cursor.fetchone()[0]
            if null_ids > 0:
                quality_issues.append(f"个人客户身份证号空值: {null_ids}个")

            # 检查账户号码
            self.cursor.execute("""
                SELECT COUNT(*) FROM tb_acc
                WHERE Self_acc_no IS NULL OR Self_acc_no = ''
            """)
            null_accnos = self.cursor.fetchone()[0]
            if null_accnos > 0:
                quality_issues.append(f"账户号码空值: {null_accnos}个")

            if not quality_issues:
                print("✅ 必填字段检查通过")
            else:
                for issue in quality_issues:
                    print(f"❌ {issue}")

        except Exception as e:
            print(f"必填字段检查失败: {e}")

        # 2. 检查数据格式
        try:
            # 检查身份证号格式（应为18位）
            self.cursor.execute("""
                SELECT COUNT(*) FROM tb_cst_pers
                WHERE Id_no NOT REGEXP '^[0-9X]{18}$'
            """)
            invalid_ids = self.cursor.fetchone()[0]
            if invalid_ids == 0:
                print("✅ 身份证号格式检查通过")
            else:
                print(f"❌ 身份证号格式错误: {invalid_ids}个")

        except Exception as e:
            print(f"身份证号格式检查失败: {e}")

        # 3. 检查业务逻辑
        try:
            # 检查交易金额合理性（不应为负数）
            self.cursor.execute("""
                SELECT COUNT(*) FROM tb_acc_txn
                WHERE Org_amt < 0
            """)
            negative_amounts = self.cursor.fetchone()[0]
            if negative_amounts == 0:
                print("✅ 交易金额合理性检查通过")
            else:
                print(f"❌ 负金额交易记录: {negative_amounts}个")

        except Exception as e:
            print(f"交易金额合理性检查失败: {e}")

        return len(quality_issues) == 0

    def check_business_rules(self):
        """检查业务规则符合性"""
        print("\n🏛️  四、300号文件业务规范检查")
        print("-" * 40)

        business_checks = []

        # 1. 检查客户年龄合理性（身份证号中的出生日期）
        try:
            self.cursor.execute("""
                SELECT Id_no, Cst_name FROM tb_cst_pers
                WHERE Id_no IS NOT NULL AND LENGTH(Id_no) = 18
                LIMIT 5
            """)
            customers = self.cursor.fetchall()

            valid_age_count = 0
            for id_no, name in customers:
                # 简单年龄检查：身份证号第7-14位为出生日期
                if id_no and len(id_no) == 18:
                    try:
                        birth_year = int(id_no[6:10])
                        current_year = 2025
                        age = current_year - birth_year
                        if 18 <= age <= 100:  # 合理年龄范围
                            valid_age_count += 1
                    except:
                        pass

            if len(customers) > 0 and valid_age_count == len(customers):
                print("✅ 客户年龄合理性检查通过")
            else:
                print(f"⚠️  客户年龄检查: {valid_age_count}/{len(customers)} 合理")

        except Exception as e:
            print(f"客户年龄检查失败: {e}")

        # 2. 检查交易类型代码
        try:
            # 检查收付标识（Lend_flag）
            self.cursor.execute("""
                SELECT DISTINCT Lend_flag FROM tb_acc_txn
                WHERE Lend_flag IS NOT NULL
            """)
            lend_flags = [row[0] for row in self.cursor.fetchall()]
            valid_flags = {'10', '11'}

            invalid_flags = [f for f in lend_flags if f not in valid_flags]
            if not invalid_flags:
                print("✅ 交易收付标识检查通过")
            else:
                print(f"❌ 无效收付标识: {invalid_flags}")

        except Exception as e:
            print(f"交易类型检查失败: {e}")

        # 3. 检查币种代码
        try:
            self.cursor.execute("""
                SELECT DISTINCT Cur FROM tb_acc_txn
                WHERE Cur IS NOT NULL
            """)
            currencies = [row[0] for row in self.cursor.fetchall()]
            valid_currencies = {'CNY', 'USD', 'EUR', 'JPY', 'GBP', 'HKD'}

            invalid_currencies = [c for c in currencies if c not in valid_currencies]
            if not invalid_currencies:
                print("✅ 币种代码检查通过")
            else:
                print(f"❌ 无效币种代码: {invalid_currencies}")

        except Exception as e:
            print(f"币种代码检查失败: {e}")

        return business_checks

    def generate_summary_report(self, volume_results, relationship_tests, quality_passed, business_checks):
        """生成总结报告"""
        print("\n" + "=" * 60)
        print("📋 五、验证总结报告")
        print("=" * 60)

        # 数据量统计
        total_records = 0
        for desc, result in volume_results.items():
            if 'actual' in result:
                total_records += result['actual']

        print(f"📊 总数据量: {total_records:,} 条记录")

        # 通过率统计
        volume_pass = len([r for r in volume_results.values() if 'PASS' in r['status']])
        relationship_pass = len([r for r in relationship_tests if 'PASS' in r[1]])

        print(f"📈 数据量达标率: {volume_pass}/{len(volume_results)} ({volume_pass/len(volume_results)*100:.1f}%)")
        print(f"🔗 关联性合格率: {relationship_pass}/{len(relationship_tests)} ({relationship_pass/len(relationship_tests)*100:.1f}%)")
        print(f"🔍 数据质量: {'✅ 通过' if quality_passed else '❌ 存在问题'}")

        # 关键指标
        print(f"\n🎯 关键业务指标:")
        print(f"   - 个人客户: {volume_results.get('个人客户', {}).get('actual', 0)} 个 (目标: 10)")
        print(f"   - 企业客户: {volume_results.get('企业客户', {}).get('actual', 0)} 个 (目标: 2)")
        print(f"   - 覆盖表数: {len([r for r in volume_results.values() if r['actual'] > 0])}/15")

        # 状态评估
        if volume_pass >= len(volume_results) * 0.8 and relationship_pass >= len(relationship_tests) * 0.8:
            status = "✅ 整体合格"
        elif volume_pass >= len(volume_results) * 0.6 and relationship_pass >= len(relationship_tests) * 0.6:
            status = "⚠️  部分合格"
        else:
            status = "❌ 需要改进"

        print(f"\n🏆 整体评估: {status}")

        # 建议
        print(f"\n💡 改进建议:")
        if volume_results.get('个人客户', {}).get('actual', 0) < 10:
            print("   - 个人客户数量未达到目标10个")
        if volume_results.get('企业客户', {}).get('actual', 0) < 2:
            print("   - 企业客户数量未达到目标2个")

        txn_count = volume_results.get('账户交易', {}).get('actual', 0)
        if txn_count < 20:
            print("   - 建议增加账户交易记录数量以测试更多场景")

        report_count = volume_results.get('大额交易报告', {}).get('actual', 0) + volume_results.get('可疑交易报告', {}).get('actual', 0)
        if report_count < 5:
            print("   - 建议增加大额和可疑交易报告数量")

    def close(self):
        """关闭数据库连接"""
        self.cursor.close()
        self.conn.close()

if __name__ == "__main__":
    validator = DataValidationReport()

    try:
        # 执行各项检查
        volume_results = validator.check_table_data_volume()
        relationship_tests = validator.check_data_relationships()
        quality_passed = validator.check_data_quality()
        business_checks = validator.check_business_rules()

        # 生成总结报告
        validator.generate_summary_report(volume_results, relationship_tests, quality_passed, business_checks)

    except Exception as e:
        print(f"验证过程中发生错误: {e}")

    validator.close()
    print(f"\n验证完成: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")