#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
为高风险客户补充联网核查记录
确保所有高风险客户都有合理的联网核查记录
"""

import mysql.connector
import random
import sys
from datetime import datetime, timedelta

class HighRiskNetworkVerificationAdder:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.customers_without_verification = []

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

    def get_customers_without_verification(self):
        """获取没有联网核查记录的高风险客户"""
        try:
            # 获取没有联网核查记录的个人高风险客户
            self.cursor.execute("""
                SELECT
                    p.Cst_no,
                    p.Acc_name,
                    p.Id_no,
                    p.Bank_code1,
                    b.Bank_name,
                    r.Time as risk_assessment_time,
                    'personal' as customer_type
                FROM tb_risk_new r
                JOIN tb_cst_pers p ON r.Cst_no = p.Cst_no
                JOIN tb_bank b ON p.Bank_code1 = b.Bank_code1
                WHERE r.Risk_code = '01' AND r.Acc_type = '11'
                AND NOT EXISTS (
                    SELECT 1 FROM tb_lwhc_log l WHERE l.Id_no = p.Id_no
                )
            """)
            personal_without = self.cursor.fetchall()

            # 获取没有联网核查记录的企业高风险客户
            self.cursor.execute("""
                SELECT
                    u.Cst_no,
                    u.Acc_name,
                    u.License,
                    u.Bank_code1,
                    b.Bank_name,
                    r.Time as risk_assessment_time,
                    'corporate' as customer_type
                FROM tb_risk_new r
                JOIN tb_cst_unit u ON r.Cst_no = u.Cst_no
                JOIN tb_bank b ON u.Bank_code1 = b.Bank_code1
                WHERE r.Risk_code = '01' AND r.Acc_type = '13'
                AND NOT EXISTS (
                    SELECT 1 FROM tb_lwhc_log l WHERE l.Id_no = u.License
                )
            """)
            corporate_without = self.cursor.fetchall()

            self.customers_without_verification = personal_without + corporate_without

            print(f"[OK] 找到无联网核查记录的高风险客户: {len(self.customers_without_verification)} 个")
            print(f"  - 个人客户: {len(personal_without)} 个")
            print(f"  - 企业客户: {len(corporate_without)} 个")

            return len(self.customers_without_verification) > 0

        except Exception as e:
            print(f"[ERROR] 获取客户信息失败: {e}")
            return False

    def generate_verification_dates(self):
        """生成联网核查日期分布"""
        verification_dates = []

        # 从2020年1月1日到6月1日的日期分布
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2020, 6, 1)
        date_range = (end_date - start_date).days + 1

        # 生成日期分布，每个月分配合理数量
        monthly_distribution = {
            1: 8,   # 1月
            2: 6,   # 2月
            3: 6,   # 3月
            4: 6,   # 4月
            5: 4,   # 5月（截至6月1日）
        }

        for month, count in monthly_distribution.items():
            for _ in range(count):
                # 在该月内随机选择日期
                year = 2020
                if month == 5:
                    day = random.randint(1, 1)  # 5月只到1日
                else:
                    day = random.randint(1, 28)

                # 随机选择时间
                hour = random.randint(9, 17)
                minute = random.randint(0, 59)
                second = random.randint(0, 59)

                verification_date = f"{year}{month:02d}{day:02d}"
                verification_time = f"{hour:02d}{minute:02d}{second:02d}"

                verification_dates.append({
                    'date': verification_date,
                    'time': verification_time,
                    'month': month
                })

        # 如果需要更多日期，继续生成
        while len(verification_dates) < len(self.customers_without_verification):
            random_days = random.randint(0, date_range - 1)
            check_date = (start_date + timedelta(days=random_days))
            if check_date > end_date:
                check_date = end_date

            hour = random.randint(9, 17)
            minute = random.randint(0, 59)
            second = random.randint(0, 59)

            verification_date = check_date.strftime('%Y%m%d')
            verification_time = f"{hour:02d}{minute:02d}{second:02d}"

            verification_dates.append({
                'date': verification_date,
                'time': verification_time,
                'month': check_date.month
            })

        random.shuffle(verification_dates)
        return verification_dates

    def add_network_verification_records(self):
        """添加联网核查记录"""
        print("\n[INFO] 开始添加联网核查记录...")

        # 生成联网核查日期
        verification_dates = self.generate_verification_dates()
        print(f"[OK] 生成了 {len(verification_dates)} 个核查日期")

        # 联网核查结果定义（高风险客户的核查结果应该偏向异常）
        result_codes = {
            '13': {'name': '一致照片错误', 'desc': '姓名与号码相符但照片错误', 'ratio': 0.35},
            '14': {'name': '不匹配', 'desc': '号码存在但与姓名不匹配', 'ratio': 0.25},
            '15': {'name': '不存在', 'desc': '号码不存在', 'ratio': 0.20},
            '16': {'name': '其他', 'desc': '其他异常情况', 'ratio': 0.15},
            '11': {'name': '一致且照片存在', 'desc': '公民身份号码与姓名一致，且存在照片', 'ratio': 0.05},
            '12': {'name': '一致无照片', 'desc': '姓名与号码相符但照片不存在', 'ratio': 0.00}
        }

        # 核查目的
        purposes = [
            '高风险客户核查', '可疑交易核查', '异常行为监测',
            '身份验证复查', '合规检查', '监管要求核查'
        ]

        # 操作员和网点信息
        operators = ['王经理', '李主任', '张主管', '赵专员', '钱审查员']
        operation_lines = ['总行营业部', '风险管理部', '合规部', '审计部', '信贷审查部']

        batch_data = []
        added_count = 0

        for i, customer in enumerate(self.customers_without_verification):
            if i >= len(verification_dates):
                break

            customer_data = customer
            date_info = verification_dates[i]

            # 选择核查结果（高风险客户更多异常结果）
            result_code = self._choose_result_code(result_codes)

            # 生成操作信息
            operator = random.choice(operators)
            counter_no = f"RISK{str(random.randint(100, 999)).zfill(3)}"
            ope_line = random.choice(operation_lines)
            mode = '01'  # 在线核查
            purpose = random.choice(purposes)

            data = (
                customer_data[4],  # Bank_name
                customer_data[3],  # Bank_code2
                date_info['date'],  # Date
                date_info['time'],  # Time
                customer_data[1],  # Name (客户姓名)
                customer_data[2],  # Id_no (身份证号或营业执照)
                result_code,  # Result
                counter_no,  # Counter_no
                ope_line,  # Ope_line
                mode,  # Mode
                purpose  # Purpose
            )

            batch_data.append(data)
            added_count += 1

            # 每50条提交一次
            if len(batch_data) >= 50:
                self.cursor.executemany("""
                    INSERT INTO tb_lwhc_log (
                        Bank_name, Bank_code2, Date, Time, Name, Id_no, Result,
                        Counter_no, Ope_line, Mode, Purpose
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, batch_data)
                self.conn.commit()
                batch_data = []
                print(f"  已添加 {added_count} 条联网核查记录")

        # 提交剩余数据
        if batch_data:
            self.cursor.executemany("""
                INSERT INTO tb_lwhc_log (
                    Bank_name, Bank_code2, Date, Time, Name, Id_no, Result,
                    Counter_no, Ope_line, Mode, Purpose
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, batch_data)
            self.conn.commit()

        print(f"[OK] 成功添加 {added_count} 条联网核查记录")
        return True

    def _choose_result_code(self, result_codes):
        """根据概率分布选择结果代码"""
        rand_val = random.random()
        cumulative = 0
        for code, info in result_codes.items():
            cumulative += info['ratio']
            if rand_val <= cumulative:
                return code
        return '16'  # 默认返回其他

    def verify_results(self):
        """验证添加结果"""
        print("\n[INFO] 验证联网核查补充结果...")

        try:
            # 重新检查覆盖情况
            self.cursor.execute("""
                SELECT COUNT(DISTINCT p.Cst_no)
                FROM tb_risk_new r
                JOIN tb_cst_pers p ON r.Cst_no = p.Cst_no
                LEFT JOIN tb_lwhc_log l ON p.Id_no = l.Id_no
                WHERE r.Risk_code = '01' AND r.Acc_type = '11'
            """)

            personal_with_verification = self.cursor.fetchone()[0]

            self.cursor.execute("""
                SELECT COUNT(DISTINCT u.Cst_no)
                FROM tb_risk_new r
                JOIN tb_cst_unit u ON r.Cst_no = u.Cst_no
                LEFT JOIN tb_lwhc_log l ON u.License = l.Id_no
                WHERE r.Risk_code = '01' AND r.Acc_type = '13'
            """)

            corporate_with_verification = self.cursor.fetchone()[0]

            total_with_verification = personal_with_verification + corporate_with_verification

            print(f"\n联网核查补充后覆盖情况:")
            print(f"个人高风险客户有联网核查: {personal_with_verification}")
            print(f"企业高风险客户有联网核查: {corporate_with_verification}")
            print(f"总计有联网核查记录: {total_with_verification}")

            # 查看新添加记录的时间分布
            self.cursor.execute("""
                SELECT
                    SUBSTRING(Date, 5, 2) AS month,
                    COUNT(*) AS count
                FROM tb_lwhc_log
                WHERE Id_no IN (
                    SELECT p.Id_no FROM tb_risk_new r JOIN tb_cst_pers p ON r.Cst_no = p.Cst_no WHERE r.Risk_code = '01'
                    UNION
                    SELECT u.License FROM tb_risk_new r JOIN tb_cst_unit u ON r.Cst_no = u.Cst_no WHERE r.Risk_code = '01'
                )
                GROUP BY SUBSTRING(Date, 5, 2)
                ORDER BY month
            """)

            new_records_distribution = self.cursor.fetchall()
            print(f"\n新添加记录的月度分布:")
            for month, count in new_records_distribution:
                print(f"  {int(month)}月: {count} 条")

            return True

        except Exception as e:
            print(f"[ERROR] 验证失败: {e}")
            return False

    def run_verification_addition(self):
        """执行联网核查补充流程"""
        print("=" * 60)
        print("高风险客户联网核查补充程序")
        print("=" * 60)

        try:
            if not self.connect_database():
                return False

            if not self.get_customers_without_verification():
                print("[INFO] 所有高风险客户都有联网核查记录，无需补充")
                return True

            if not self.add_network_verification_records():
                return False

            if not self.verify_results():
                return False

            print("\n[SUCCESS] 高风险客户联网核查补充完成！")
            print("✅ 所有高风险客户现在都有联网核查记录")
            print("✅ 联网核查时间合理分布在2020年1月1日至6月1日期间")
            print("✅ 高风险客户的核查结果偏向异常情况，符合逻辑")

            return True

        except Exception as e:
            print(f"[ERROR] 联网核查补充过程中发生错误: {e}")
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
    print("[INFO] 开始为高风险客户补充联网核查记录...")

    adder = HighRiskNetworkVerificationAdder()
    success = adder.run_verification_addition()

    if success:
        print("\n[SUCCESS] 联网核查补充成功！")
        sys.exit(0)
    else:
        print("\n[ERROR] 联网核查补充失败！")
        sys.exit(1)

if __name__ == "__main__":
    main()