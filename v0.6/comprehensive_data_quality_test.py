#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AML300数据完整性和业务逻辑测试程序
测试15张表的完整性和逻辑性
"""

import mysql.connector
import sys
from datetime import datetime

class ComprehensiveDataQualityTest:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.test_results = []

    def connect_database(self):
        """连接数据库"""
        try:
            self.conn = mysql.connector.connect(
                host='101.42.102.9',
                port=3306,
                user='root',
                password='Bancstone123!',
                database='AML300',
                charset='utf8mb4'
            )
            self.cursor = self.conn.cursor()
            print("[OK] 数据库连接成功")
            return True
        except Exception as e:
            print(f"[ERROR] 数据库连接失败: {e}")
            return False

    def test_table_integrity(self):
        """测试表完整性"""
        print("\n" + "="*60)
        print("第一部分：表完整性测试")
        print("="*60)

        expected_tables = [
            'tb_bank', 'tb_settle_type', 'tb_cst_pers', 'tb_cst_unit', 'tb_acc',
            'tb_acc_txn', 'tb_cross_border', 'tb_cred_txn', 'tb_cash_remit',
            'tb_cash_convert', 'tb_risk_new', 'tb_risk_his', 'tb_lwhc_log',
            'tb_lar_report', 'tb_sus_report'
        ]

        tables_found = []
        tables_missing = []

        for table in expected_tables:
            try:
                self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = self.cursor.fetchone()[0]
                tables_found.append((table, count))
                print(f"  [OK] {table:20} : {count:>8,} 条记录")
            except Exception as e:
                tables_missing.append(table)
                print(f"  [ERROR] {table:20} : 表不存在或查询失败 - {e}")

        # 计算完整性得分
        integrity_score = len(tables_found) / len(expected_tables) * 100
        self.test_results.append({
            'test': '表完整性测试',
            'score': integrity_score,
            'details': f'找到表: {len(tables_found)}/{len(expected_tables)}'
        })

        print(f"\n[INFO] 表完整性得分: {integrity_score:.1f}/100")
        return len(tables_found) == len(expected_tables)

    def test_data_volume_requirements(self):
        """测试数据量要求"""
        print("\n" + "="*60)
        print("第二部分：数据量要求测试")
        print("="*60)

        volume_requirements = [
            ('tb_cst_pers', '个人客户', 1000, '>='),
            ('tb_cst_unit', '企业客户', 100, '>='),
            ('tb_acc_txn', '账户交易', 10000, '>='),
            ('tb_cross_border', '跨境交易', 100, '>='),
            ('tb_cred_txn', '信用卡交易', 100, '>='),
            ('tb_cash_remit', '现金汇款', 100, '>='),
            ('tb_lar_report', '大额交易报告', 50, '>='),
            ('tb_sus_report', '可疑交易报告', 30, '>=')
        ]

        volume_passed = 0
        total_volume_tests = len(volume_requirements)

        for table, desc, target, operator in volume_requirements:
            try:
                self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = self.cursor.fetchone()[0]

                if operator == '>=':
                    passed = count >= target
                else:
                    passed = count == target

                status = "[OK]" if passed else "[FAIL]"
                print(f"  {status} {desc:12} : {count:>8,} (目标: {operator}{target})")

                if passed:
                    volume_passed += 1

            except Exception as e:
                print(f"  [ERROR] {desc:12} : 查询失败 - {e}")

        volume_score = volume_passed / total_volume_tests * 100
        self.test_results.append({
            'test': '数据量要求测试',
            'score': volume_score,
            'details': f'达标: {volume_passed}/{total_volume_tests}'
        })

        print(f"\n[INFO] 数据量要求得分: {volume_score:.1f}/100")
        return volume_score >= 80

    def test_business_logic_consistency(self):
        """测试业务逻辑一致性"""
        print("\n" + "="*60)
        print("第三部分：业务逻辑一致性测试")
        print("="*60)

        logic_tests = []
        logic_passed = 0

        # 测试1: 客户-账户关联性
        print("  测试1: 客户-账户关联性")
        try:
            self.cursor.execute("""
                SELECT COUNT(*) FROM tb_acc a
                WHERE a.Cst_no NOT IN (
                    SELECT Cst_no FROM tb_cst_pers
                    UNION SELECT Cst_no FROM tb_cst_unit
                )
            """)
            orphan_accounts = self.cursor.fetchone()[0]

            if orphan_accounts == 0:
                print("    [OK] 所有账户都有对应的客户")
                logic_passed += 1
                logic_tests.append(("客户-账户关联", "通过", "无孤立账户"))
            else:
                print(f"    [FAIL] 发现 {orphan_accounts} 个孤立账户")
                logic_tests.append(("客户-账户关联", "失败", f"{orphan_accounts} 个孤立账户"))
        except Exception as e:
            print(f"    [ERROR] 测试失败: {e}")
            logic_tests.append(("客户-账户关联", "错误", str(e)))

        # 测试2: 风险-客户关联性
        print("  测试2: 风险-客户关联性")
        try:
            self.cursor.execute("""
                SELECT COUNT(*) FROM tb_risk_new r
                WHERE r.Cst_no NOT IN (
                    SELECT Cst_no FROM tb_cst_pers
                    UNION SELECT Cst_no FROM tb_cst_unit
                )
            """)
            orphan_risks = self.cursor.fetchone()[0]

            if orphan_risks == 0:
                print("    [OK] 所有风险记录都有对应的客户")
                logic_passed += 1
                logic_tests.append(("风险-客户关联", "通过", "无孤立风险记录"))
            else:
                print(f"    [FAIL] 发现 {orphan_risks} 个孤立风险记录")
                logic_tests.append(("风险-客户关联", "失败", f"{orphan_risks} 个孤立风险记录"))
        except Exception as e:
            print(f"    [ERROR] 测试失败: {e}")
            logic_tests.append(("风险-客户关联", "错误", str(e)))

        # 测试3: 交易时间范围（2020年1-6月）
        print("  测试3: 交易时间范围")
        try:
            self.cursor.execute("""
                SELECT MIN(Date), MAX(Date), COUNT(*)
                FROM tb_acc_txn
                WHERE Date BETWEEN '20200101' AND '20200630'
            """)
            txn_result = self.cursor.fetchone()

            if txn_result[0] and txn_result[1]:
                print(f"    [OK] 交易时间范围: {txn_result[0]} - {txn_result[1]}, 共 {txn_result[2]} 条")
                logic_passed += 1
                logic_tests.append(("交易时间范围", "通过", f"{txn_result[0]} - {txn_result[1]}"))
            else:
                print("    [FAIL] 交易时间范围异常")
                logic_tests.append(("交易时间范围", "失败", "时间范围异常"))
        except Exception as e:
            print(f"    [ERROR] 测试失败: {e}")
            logic_tests.append(("交易时间范围", "错误", str(e)))

        # 测试4: 开户时间范围（2010-2025年）
        print("  测试4: 开户时间范围")
        try:
            self.cursor.execute("""
                SELECT MIN(Open_time), MAX(Open_time) FROM tb_cst_pers
                WHERE Open_time BETWEEN '20100101' AND '20250101'
            """)
            open_result = self.cursor.fetchone()

            if open_result[0] and open_result[1]:
                print(f"    [OK] 个人客户开户时间: {open_result[0]} - {open_result[1]}")

                self.cursor.execute("""
                    SELECT MIN(Open_time), MAX(Open_time) FROM tb_cst_unit
                    WHERE Open_time BETWEEN '20100101' AND '20250101'
                """)
                corp_result = self.cursor.fetchone()

                if corp_result[0] and corp_result[1]:
                    print(f"    [OK] 企业客户开户时间: {corp_result[0]} - {corp_result[1]}")
                    logic_passed += 1
                    logic_tests.append(("开户时间范围", "通过", f"个人:{open_result[0]}-{open_result[1]}, 企业:{corp_result[0]}-{corp_result[1]}"))
                else:
                    print("    [FAIL] 企业客户开户时间异常")
                    logic_tests.append(("开户时间范围", "失败", "企业客户时间异常"))
            else:
                print("    [FAIL] 个人客户开户时间异常")
                logic_tests.append(("开户时间范围", "失败", "个人客户时间异常"))
        except Exception as e:
            print(f"    [ERROR] 测试失败: {e}")
            logic_tests.append(("开户时间范围", "错误", str(e)))

        # 测试5: 风险等级分布
        print("  测试5: 风险等级分布")
        try:
            self.cursor.execute("""
                SELECT Risk_code, COUNT(*) as count
                FROM tb_risk_new
                GROUP BY Risk_code
                ORDER BY Risk_code
            """)
            risk_distribution = self.cursor.fetchall()
            total_risks = sum(count for _, count in risk_distribution)

            print("    风险等级分布:")
            expected_distribution = {'01': 0.05, '02': 0.15, '03': 0.50, '04': 0.30}
            distribution_ok = True

            for risk_code, count in risk_distribution:
                actual_ratio = count / total_risks
                expected_ratio = expected_distribution.get(risk_code, 0)

                if abs(actual_ratio - expected_ratio) < 0.2:  # 允许20%误差
                    print(f"      [OK] 风险等级 {risk_code}: {count} ({actual_ratio:.1%})")
                else:
                    print(f"      [WARN] 风险等级 {risk_code}: {count} ({actual_ratio:.1%}) - 期望 {expected_ratio:.1%}")
                    distribution_ok = False

            if distribution_ok:
                print("    [OK] 风险等级分布合理")
                logic_passed += 1
                logic_tests.append(("风险等级分布", "通过", "分布合理"))
            else:
                print("    [WARN] 风险等级分布有偏差")
                logic_tests.append(("风险等级分布", "警告", "分布有偏差"))
        except Exception as e:
            print(f"    [ERROR] 测试失败: {e}")
            logic_tests.append(("风险等级分布", "错误", str(e)))

        logic_score = logic_passed / 5 * 100
        self.test_results.append({
            'test': '业务逻辑一致性测试',
            'score': logic_score,
            'details': f'通过: {logic_passed}/5'
        })

        print(f"\n[INFO] 业务逻辑一致性得分: {logic_score:.1f}/100")
        return logic_score >= 80

    def test_data_quality(self):
        """测试数据质量"""
        print("\n" + "="*60)
        print("第四部分：数据质量测试")
        print("="*60)

        quality_tests = []
        quality_passed = 0

        # 测试1: 必填字段完整性
        print("  测试1: 必填字段完整性")
        try:
            # 检查个人客户必填字段
            self.cursor.execute("""
                SELECT COUNT(*) FROM tb_cst_pers
                WHERE Cst_no IS NULL OR Cst_no = '' OR
                      Acc_name IS NULL OR Acc_name = '' OR
                      Id_no IS NULL OR Id_no = ''
            """)
            missing_required = self.cursor.fetchone()[0]

            if missing_required == 0:
                print("    [OK] 个人客户必填字段完整")
                quality_passed += 1
                quality_tests.append(("个人客户必填字段", "通过"))
            else:
                print(f"    [FAIL] 个人客户有 {missing_required} 条记录缺少必填字段")
                quality_tests.append(("个人客户必填字段", "失败", f"{missing_required} 条记录有问题"))

        except Exception as e:
            print(f"    [ERROR] 测试失败: {e}")
            quality_tests.append(("个人客户必填字段", "错误", str(e)))

        # 测试2: 交易金额合理性
        print("  测试2: 交易金额合理性")
        try:
            self.cursor.execute("""
                SELECT COUNT(*) FROM tb_acc_txn
                WHERE Org_amt <= 0 OR Org_amt > 100000000
            """)
            unreasonable_amounts = self.cursor.fetchone()[0]

            if unreasonable_amounts == 0:
                print("    [OK] 交易金额合理")
                quality_passed += 1
                quality_tests.append(("交易金额合理性", "通过"))
            else:
                print(f"    [WARN] 有 {unreasonable_amounts} 条交易金额异常")
                quality_tests.append(("交易金额合理性", "警告", f"{unreasonable_amounts} 条记录异常"))
        except Exception as e:
            print(f"    [ERROR] 测试失败: {e}")
            quality_tests.append(("交易金额合理性", "错误", str(e)))

        # 测试3: 身份证号码格式
        print("  测试3: 身份证号码格式")
        try:
            self.cursor.execute("""
                SELECT COUNT(*) FROM tb_cst_pers
                WHERE Id_type = '11' AND
                      (LENGTH(Id_no) != 18 OR Id_no NOT REGEXP '^[0-9]{17}[0-9X]')
            """)
            invalid_id_cards = self.cursor.fetchone()[0]

            if invalid_id_cards == 0:
                print("    [OK] 身份证号码格式正确")
                quality_passed += 1
                quality_tests.append(("身份证号码格式", "通过"))
            else:
                print(f"    [WARN] 有 {invalid_id_cards} 个身份证号码格式异常")
                quality_tests.append(("身份证号码格式", "警告", f"{invalid_id_cards} 个号码异常"))
        except Exception as e:
            print(f"    [ERROR] 测试失败: {e}")
            quality_tests.append(("身份证号码格式", "错误", str(e)))

        # 测试4: 日期格式一致性
        print("  测试4: 日期格式一致性")
        try:
            self.cursor.execute("""
                SELECT COUNT(*) FROM tb_acc_txn
                WHERE Date NOT REGEXP '^[0-9]{8}$' OR
                      Time NOT REGEXP '^[0-9]{6}$'
            """)
            invalid_dates = self.cursor.fetchone()[0]

            if invalid_dates == 0:
                print("    [OK] 日期格式一致")
                quality_passed += 1
                quality_tests.append(("日期格式一致性", "通过"))
            else:
                print(f"    [WARN] 有 {invalid_dates} 条记录日期格式异常")
                quality_tests.append(("日期格式一致性", "警告", f"{invalid_dates} 条记录异常"))
        except Exception as e:
            print(f"    [ERROR] 测试失败: {e}")
            quality_tests.append(("日期格式一致性", "错误", str(e)))

        quality_score = quality_passed / 4 * 100
        self.test_results.append({
            'test': '数据质量测试',
            'score': quality_score,
            'details': f'通过: {quality_passed}/4'
        })

        print(f"\n[INFO] 数据质量得分: {quality_score:.1f}/100")
        return quality_score >= 80

    def test_compliance_with_300no(self):
        """测试300号文合规性"""
        print("\n" + "="*60)
        print("第五部分：300号文合规性测试")
        print("="*60)

        compliance_tests = []
        compliance_passed = 0

        # 测试1: 15张表结构完整性
        print("  测试1: 15张表结构完整性")
        try:
            self.cursor.execute("SHOW TABLES")
            existing_tables = [table[0] for table in self.cursor.fetchall()]

            required_tables = {
                'tb_bank', 'tb_settle_type', 'tb_cst_pers', 'tb_cst_unit', 'tb_acc',
                'tb_acc_txn', 'tb_cross_border', 'tb_cred_txn', 'tb_cash_remit',
                'tb_cash_convert', 'tb_risk_new', 'tb_risk_his', 'tb_lwhc_log',
                'tb_lar_report', 'tb_sus_report'
            }

            missing_tables = required_tables - set(existing_tables)

            if len(missing_tables) == 0:
                print("    [OK] 300号文要求的15张表全部存在")
                compliance_passed += 1
                compliance_tests.append(("15张表完整性", "通过"))
            else:
                print(f"    [FAIL] 缺少表: {missing_tables}")
                compliance_tests.append(("15张表完整性", "失败", f"缺少: {missing_tables}"))
        except Exception as e:
            print(f"    [ERROR] 测试失败: {e}")
            compliance_tests.append(("15张表完整性", "错误", str(e)))

        # 测试2: 个人客户关键字段
        print("  测试2: 个人客户关键字段")
        try:
            self.cursor.execute("DESCRIBE tb_cst_pers")
            personal_fields = [field[0] for field in self.cursor.fetchall()]

            required_personal_fields = ['Cst_no', 'Id_no', 'Cst_sex', 'Nation', 'Id_type', 'Id_deadline', 'Occupation', 'Income', 'Contact1', 'Address1']
            missing_fields = [field for field in required_personal_fields if field not in personal_fields]

            if len(missing_fields) == 0:
                print("    [OK] 个人客户关键字段齐全")
                compliance_passed += 1
                compliance_tests.append(("个人客户关键字段", "通过"))
            else:
                print(f"    [FAIL] 个人客户缺少字段: {missing_fields}")
                compliance_tests.append(("个人客户关键字段", "失败", f"缺少: {missing_fields}"))
        except Exception as e:
            print(f"    [ERROR] 测试失败: {e}")
            compliance_tests.append(("个人客户关键字段", "错误", str(e)))

        # 测试3: 交易记录关键字段
        print("  测试3: 交易记录关键字段")
        try:
            self.cursor.execute("DESCRIBE tb_acc_txn")
            txn_fields = [field[0] for field in self.cursor.fetchall()]

            required_txn_fields = ['Date', 'Time', 'Self_acc_no', 'Cst_no', 'Id_no', 'Lend_flag', 'Tsf_flag', 'Cur', 'Org_amt', 'Usd_amt', 'Rmb_amt']
            missing_fields = [field for field in required_txn_fields if field not in txn_fields]

            if len(missing_fields) == 0:
                print("    [OK] 交易记录关键字段齐全")
                compliance_passed += 1
                compliance_tests.append(("交易记录关键字段", "通过"))
            else:
                print(f"    [FAIL] 交易记录缺少字段: {missing_fields}")
                compliance_tests.append(("交易记录关键字段", "失败", f"缺少: {missing_fields}"))
        except Exception as e:
            print(f"    [ERROR] 测试失败: {e}")
            compliance_tests.append(("交易记录关键字段", "错误", str(e)))

        compliance_score = compliance_passed / 3 * 100
        self.test_results.append({
            'test': '300号文合规性测试',
            'score': compliance_score,
            'details': f'通过: {compliance_passed}/3'
        })

        print(f"\n[INFO] 300号文合规性得分: {compliance_score:.1f}/100")
        return compliance_score >= 90

    def generate_final_report(self):
        """生成最终测试报告"""
        print("\n" + "="*80)
        print("AML300数据库完整性和业务逻辑测试最终报告")
        print("="*80)
        print(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)

        # 计算总体得分
        total_score = 0
        max_score = 0

        for result in self.test_results:
            total_score += result['score']
            max_score += 100

        overall_score = total_score / max_score * 100

        # 显示详细结果
        print("\n[测试结果详情]")
        print("-" * 80)
        for i, result in enumerate(self.test_results, 1):
            status = "优秀" if result['score'] >= 90 else "良好" if result['score'] >= 80 else "需改进" if result['score'] >= 60 else "不合格"
            print(f"{i}. {result['test']:20} : {result['score']:6.1f}/100 ({status:6}) - {result['details']}")

        # 总体评估
        print(f"\n[总体评估]")
        print("-" * 80)
        print(f"综合得分: {overall_score:.1f}/100")

        if overall_score >= 90:
            grade = "A"
            assessment = "优秀 - 系统完全符合要求"
            recommendation = "系统可以投入使用"
        elif overall_score >= 80:
            grade = "B"
            assessment = "良好 - 系统基本符合要求"
            recommendation = "建议进行小幅优化后投入使用"
        elif overall_score >= 70:
            grade = "C"
            assessment = "一般 - 系统部分符合要求"
            recommendation = "需要进行重要改进"
        else:
            grade = "D"
            assessment = "不合格 - 系统存在重大问题"
            recommendation = "需要重新设计和实施"

        print(f"评级等级: {grade}")
        print(f"评估结果: {assessment}")
        print(f"改进建议: {recommendation}")

        # 数据统计摘要
        print(f"\n[数据统计摘要]")
        print("-" * 80)
        try:
            # 获取各表记录数
            tables = [
                ('tb_cst_pers', '个人客户'),
                ('tb_cst_unit', '企业客户'),
                ('tb_acc', '账户'),
                ('tb_risk_new', '最新风险等级'),
                ('tb_risk_his', '历史风险等级'),
                ('tb_acc_txn', '账户交易'),
                ('tb_cross_border', '跨境交易'),
                ('tb_cred_txn', '信用卡交易'),
                ('tb_cash_remit', '现金汇款'),
                ('tb_cash_convert', '现钞兑换'),
                ('tb_lwhc_log', '联网核查日志'),
                ('tb_lar_report', '大额交易报告'),
                ('tb_sus_report', '可疑交易报告')
            ]

            total_records = 0
            for table, desc in tables:
                try:
                    self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = self.cursor.fetchone()[0]
                    total_records += count
                    print(f"{desc:12} : {count:>8,} 条记录")
                except:
                    print(f"{desc:12} : 查询失败")

            print("-" * 80)
            print(f"总记录数: {total_records:,}")

        except Exception as e:
            print(f"数据统计失败: {e}")

        # 测试结论
        print(f"\n[测试结论]")
        print("-" * 80)
        if overall_score >= 80:
            print("✓ AML300数据库系统通过了完整性和业务逻辑测试")
            print("✓ 数据结构符合300号文件要求")
            print("✓ 业务逻辑基本合理")
            print("✓ 数据质量可接受")
            print("✓ 可以支持反洗钱监测分析业务需求")
        else:
            print("✗ AML300数据库系统未完全通过测试")
            print("✗ 需要进一步改进数据质量和业务逻辑")
            print("✗ 建议在投入使用前完成必要修复")

        print("="*80)
        return overall_score >= 80

    def run_comprehensive_test(self):
        """执行完整测试流程"""
        print("[INFO] 开始AML300数据库完整性和业务逻辑测试")
        print("="*80)

        try:
            # 连接数据库
            if not self.connect_database():
                return False

            # 执行各项测试
            self.test_table_integrity()
            self.test_data_volume_requirements()
            self.test_business_logic_consistency()
            self.test_data_quality()
            self.test_compliance_with_300no()

            # 生成最终报告
            success = self.generate_final_report()

            return success

        except Exception as e:
            print(f"[ERROR] 测试过程中发生错误: {e}")
            return False

        finally:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()

def main():
    """主函数"""
    tester = ComprehensiveDataQualityTest()
    success = tester.run_comprehensive_test()

    if success:
        print("\n[SUCCESS] AML300数据库测试通过！")
        print("系统已准备就绪，可以投入使用。")
        sys.exit(0)
    else:
        print("\n[FAIL] AML300数据库测试未完全通过！")
        print("请根据测试结果进行必要改进。")
        sys.exit(1)

if __name__ == "__main__":
    main()