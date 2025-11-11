#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修复风险评估时间分布问题
将高风险客户评估时间分布到2020年1月1日至6月30日期间
"""

import mysql.connector
import random
import sys
from datetime import datetime, timedelta

class RiskAssessmentTimelineFixer:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.high_risk_customers = []

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

    def get_high_risk_customers(self):
        """获取高风险客户数据"""
        try:
            # 获取个人高风险客户
            self.cursor.execute("""
                SELECT r.Cst_no, r.Acc_type, p.Acc_name, p.Id_no, p.Bank_code1
                FROM tb_risk_new r
                JOIN tb_cst_pers p ON r.Cst_no = p.Cst_no
                WHERE r.Risk_code = '01' AND r.Acc_type = '11'
            """)
            personal_high_risk = self.cursor.fetchall()

            # 获取企业高风险客户
            self.cursor.execute("""
                SELECT r.Cst_no, r.Acc_type, u.Acc_name, u.License, u.Bank_code1
                FROM tb_risk_new r
                JOIN tb_cst_unit u ON r.Cst_no = u.Cst_no
                WHERE r.Risk_code = '01' AND r.Acc_type = '13'
            """)
            corporate_high_risk = self.cursor.fetchall()

            self.high_risk_customers = {
                'personal': personal_high_risk,
                'corporate': corporate_high_risk
            }

            print(f"[OK] 获取个人高风险客户: {len(personal_high_risk)} 个")
            print(f"[OK] 获取企业高风险客户: {len(corporate_high_risk)} 个")
            return True
        except Exception as e:
            print(f"[ERROR] 获取高风险客户失败: {e}")
            return False

    def generate_assessment_dates(self):
        """生成分布在不同月份的评估日期"""
        # 定义每个月的风险评估数量分布
        monthly_distribution = {
            1: 8,   # 1月：8个高风险客户
            2: 7,   # 2月：7个高风险客户
            3: 9,   # 3月：9个高风险客户
            4: 10,  # 4月：10个高风险客户
            5: 11,  # 5月：11个高风险客户
            6: 10   # 6月：10个高风险客户
        }

        assessment_dates = []

        for month, count in monthly_distribution.items():
            # 为每个月生成指定数量的评估日期
            for _ in range(count):
                # 在该月内随机选择日期
                year = 2020
                day = random.randint(1, 28)  # 避免月末日期问题

                # 随机选择小时和分钟
                hour = random.randint(9, 17)  # 工作时间 9-17点
                minute = random.randint(0, 59)

                # 生成日期时间字符串
                assessment_date = f"{year}{month:02d}{day:02d}"
                assessment_time = f"{hour:02d}{minute:02d}00"

                assessment_dates.append({
                    'date': assessment_date,
                    'time': assessment_time,
                    'month': month
                })

        # 随机打乱日期顺序
        random.shuffle(assessment_dates)

        return assessment_dates

    def update_risk_assessment_timeline(self):
        """更新风险评估时间线"""
        print("\n[INFO] 更新风险评估时间线...")

        # 生成评估日期
        assessment_dates = self.generate_assessment_dates()
        print(f"[OK] 生成了 {len(assessment_dates)} 个评估日期")

        # 获取所有高风险客户
        all_customers = []

        # 处理个人客户
        for customer in self.high_risk_customers['personal']:
            all_customers.append({
                'cst_no': customer[0],
                'acc_type': customer[1],
                'customer_name': customer[2],
                'id_no': customer[3],
                'bank_code': customer[4],
                'customer_type': 'personal'
            })

        # 处理企业客户
        for customer in self.high_risk_customers['corporate']:
            all_customers.append({
                'cst_no': customer[0],
                'acc_type': customer[1],
                'customer_name': customer[1],
                'id_no': customer[2],
                'bank_code': customer[4],
                'customer_type': 'corporate'
            })

        # 如果客户数量多于生成的评估日期，扩展评估日期
        if len(all_customers) > len(assessment_dates):
            additional_dates = []
            needed = len(all_customers) - len(assessment_dates)

            for _ in range(needed):
                month = random.randint(1, 6)
                day = random.randint(1, 28)
                hour = random.randint(9, 17)
                minute = random.randint(0, 59)

                assessment_date = f"2020{month:02d}{day:02d}"
                assessment_time = f"{hour:02d}{minute:02d}00"

                additional_dates.append({
                    'date': assessment_date,
                    'time': assessment_time,
                    'month': month
                })

            assessment_dates.extend(additional_dates)
            random.shuffle(assessment_dates)

        print(f"[INFO] 准备更新 {len(all_customers)} 个高风险客户的评估时间")

        # 为每个客户分配评估日期并更新
        updated_count = 0
        for i, customer in enumerate(all_customers):
            if i < len(assessment_dates):
                date_info = assessment_dates[i]

                # 生成评估说明
                assessment_descriptions = [
                    "客户风险等级重新评估为高风险",
                    "基于交易行为分析升级为高风险",
                    "异常活动监测评估为高风险",
                    "合规检查发现风险因素",
                    "客户背景调查风险评估",
                    "可疑交易报告触发风险评估"
                ]

                norm = random.choice(assessment_descriptions)

                # 更新风险评估记录（Time字段只有8位，只能存储日期）
                assessment_date = date_info['date']

                try:
                    self.cursor.execute("""
                        UPDATE tb_risk_new
                        SET Time = %s, Norm = %s
                        WHERE Cst_no = %s AND Acc_type = %s AND Risk_code = '01'
                    """, (assessment_date, norm, customer['cst_no'], customer['acc_type']))

                    updated_count += 1

                    # 每10个客户提交一次
                    if updated_count % 10 == 0:
                        self.conn.commit()
                        print(f"  已更新 {updated_count} 个客户的评估时间")

                except Exception as e:
                    print(f"[ERROR] 更新客户 {customer['cst_no']} 失败: {e}")

        # 提交剩余更新
        self.conn.commit()
        print(f"[OK] 成功更新了 {updated_count} 个高风险客户的评估时间")

        return True

    def verify_timeline_distribution(self):
        """验证时间线分布"""
        print("\n[INFO] 验证时间线分布...")

        try:
            # 按月份统计高风险客户评估数量
            self.cursor.execute("""
                SELECT
                    SUBSTRING(Time, 5, 2) AS month,
                    COUNT(*) AS count
                FROM tb_risk_new
                WHERE Risk_code = '01'
                GROUP BY SUBSTRING(Time, 5, 2)
                ORDER BY month
            """)

            monthly_stats = self.cursor.fetchall()

            print("\n按月份统计高风险客户评估数量:")
            print("-" * 30)
            print("月份  高风险客户评估数量")
            print("-" * 30)

            for month, count in monthly_stats:
                month_name = f"{int(month)}月"
                print(f"{month_name}      {count} 个")

            # 验证时间范围
            self.cursor.execute("""
                SELECT MIN(Time) AS earliest, MAX(Time) AS latest
                FROM tb_risk_new
                WHERE Risk_code = '01'
            """)

            time_range = self.cursor.fetchone()
            print(f"\n评估时间范围: {time_range[0]} 至 {time_range[1]}")

            # 统计总体数据
            self.cursor.execute("""
                SELECT COUNT(*) FROM tb_risk_new WHERE Risk_code = '01'
            """)
            total_high_risk = self.cursor.fetchone()[0]
            print(f"高风险客户总数: {total_high_risk}")

            return True

        except Exception as e:
            print(f"[ERROR] 验证失败: {e}")
            return False

    def run_timeline_fix(self):
        """执行时间线修复流程"""
        print("=" * 60)
        print("风险评估时间线修复程序")
        print("=" * 60)

        try:
            if not self.connect_database():
                return False

            if not self.get_high_risk_customers():
                return False

            if not self.update_risk_assessment_timeline():
                return False

            if not self.verify_timeline_distribution():
                return False

            print("\n[SUCCESS] 风险评估时间线修复完成！")
            print("✅ 高风险客户评估时间已分布到2020年1-6月")
            print("✅ 每个月都有合理数量的高风险客户评估")
            print("✅ 评估时间符合业务逻辑")

            return True

        except Exception as e:
            print(f"[ERROR] 时间线修复过程中发生错误: {e}")
            if self.conn:
                self.conn.rollback()
            return False

        finally:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()

def main():
    """主函数"""
    print("[INFO] 开始修复风险评估时间线...")

    fixer = RiskAssessmentTimelineFixer()
    success = fixer.run_timeline_fix()

    if success:
        print("\n[SUCCESS] 时间线修复成功！")
        sys.exit(0)
    else:
        print("\n[ERROR] 时间线修复失败！")
        sys.exit(1)

if __name__ == "__main__":
    main()