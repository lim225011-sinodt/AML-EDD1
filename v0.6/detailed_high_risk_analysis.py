#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
详细的高风险客户分析
列出所有高风险客户的月份分布
"""

import mysql.connector
import sys
from datetime import datetime

def detailed_high_risk_analysis():
    """详细的高风险客户分析"""
    print("="*80)
    print("高风险客户详细分析报告")
    print("="*80)

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

        # 1. 按月份统计高风险客户数量
        print("\n[1] 高风险客户月度分布统计")
        print("-" * 50)

        cursor.execute("""
            SELECT
                SUBSTRING(Time, 5, 2) AS month,
                COUNT(*) AS high_risk_count,
                ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM tb_risk_new WHERE Risk_code = '01'), 2) AS percentage
            FROM tb_risk_new
            WHERE Risk_code = '01'
            GROUP BY SUBSTRING(Time, 5, 2)
            ORDER BY month
        """)

        monthly_stats = cursor.fetchall()
        print(f"{'月份':<6} {'高风险客户数量':<12} {'占比(%)':<8}")
        print("-" * 30)

        total_customers = 0
        for month, count, percentage in monthly_stats:
            month_name = f"{int(month)}月"
            print(f"{month_name:<6} {count:<12} {percentage:<8}%")
            total_customers += count

        print("-" * 30)
        print(f"{'总计':<6} {total_customers:<12} {'100.00%':<8}")

        # 2. 详细列出每个高风险客户的评估时间
        print("\n[2] 个人高风险客户详细列表")
        print("-" * 90)

        cursor.execute("""
            SELECT
                p.Cst_no AS 客户号,
                p.Acc_name AS 客户名称,
                p.Id_no AS 证件号码,
                r.Time AS 评估时间,
                SUBSTRING(r.Time, 5, 2) AS 月份,
                r.Norm AS 评估说明,
                b.Bank_name AS 分行名称
            FROM tb_risk_new r
            JOIN tb_cst_pers p ON r.Cst_no = p.Cst_no
            JOIN tb_bank b ON r.Bank_code1 = b.Bank_code1
            WHERE r.Risk_code = '01' AND r.Acc_type = '11'
            ORDER BY r.Time
        """)

        personal_customers = cursor.fetchall()
        print(f"{'客户号':<10} {'客户名称':<10} {'评估时间':<8} {'月份':<4} {'评估说明':<25} {'分行名称'}")
        print("-" * 90)

        personal_month_count = {}
        for customer in personal_customers:
            cst_no, name, id_no, time, month, norm, bank_name = customer
            month_int = int(month)
            personal_month_count[month_int] = personal_month_count.get(month_int, 0) + 1
            print(f"{cst_no:<10} {name:<10} {time:<8} {month}月   {norm[:25]:<25} {bank_name}")

        print(f"\n个人高风险客户总数: {len(personal_customers)} 个")

        # 3. 企业高风险客户详细列表
        print("\n[3] 企业高风险客户详细列表")
        print("-" * 90)

        cursor.execute("""
            SELECT
                u.Cst_no AS 客户号,
                u.Acc_name AS 企业名称,
                u.License AS 营业执照,
                r.Time AS 评估时间,
                SUBSTRING(r.Time, 5, 2) AS 月份,
                r.Norm AS 评估说明,
                b.Bank_name AS 分行名称
            FROM tb_risk_new r
            JOIN tb_cst_unit u ON r.Cst_no = u.Cst_no
            JOIN tb_bank b ON r.Bank_code1 = b.Bank_code1
            WHERE r.Risk_code = '01' AND r.Acc_type = '13'
            ORDER BY r.Time
        """)

        corporate_customers = cursor.fetchall()
        print(f"{'客户号':<10} {'企业名称':<25} {'评估时间':<8} {'月份':<4} {'评估说明':<25} {'分行名称'}")
        print("-" * 90)

        corporate_month_count = {}
        for customer in corporate_customers:
            cst_no, name, license_no, time, month, norm, bank_name = customer
            month_int = int(month)
            corporate_month_count[month_int] = corporate_month_count.get(month_int, 0) + 1
            print(f"{cst_no:<10} {name[:25]:<25} {time:<8} {month}月   {norm[:25]:<25} {bank_name}")

        print(f"\n企业高风险客户总数: {len(corporate_customers)} 个")

        # 4. 按月份分类统计
        print("\n[4] 按月份分类统计")
        print("-" * 60)

        print(f"{'月份':<6} {'个人客户':<8} {'企业客户':<8} {'当月合计':<8} {'累计占比(%)':<10}")
        print("-" * 60)

        cumulative_count = 0
        for month in range(1, 7):
            month_str = f"{month:02d}"
            personal_count = personal_month_count.get(month, 0)
            corporate_count = corporate_month_count.get(month, 0)
            month_total = personal_count + corporate_count
            cumulative_count += month_total
            cumulative_percentage = (cumulative_count / total_customers) * 100

            print(f"{month}月    {personal_count:<8} {corporate_count:<8} {month_total:<8} {cumulative_percentage:<10.2f}")

        # 5. 评估说明分类统计
        print("\n[5] 评估说明分类统计")
        print("-" * 50)

        cursor.execute("""
            SELECT
                Norm AS 评估说明,
                COUNT(*) AS 数量
            FROM tb_risk_new
            WHERE Risk_code = '01'
            GROUP BY Norm
            ORDER BY COUNT(*) DESC
        """)

        norm_stats = cursor.fetchall()
        print(f"{'评估说明':<30} {'数量':<8}")
        print("-" * 40)

        for norm, count in norm_stats:
            print(f"{norm[:30]:<30} {count:<8}")

        # 6. 时间线分布可视化
        print("\n[6] 高风险客户评估时间线分布图")
        print("-" * 60)

        # 重新获取按月统计用于可视化
        cursor.execute("""
            SELECT
                SUBSTRING(Time, 5, 2) AS month,
                COUNT(*) AS count
            FROM tb_risk_new
            WHERE Risk_code = '01'
            GROUP BY SUBSTRING(Time, 5, 2)
            ORDER BY month
        """)

        monthly_data = cursor.fetchall()

        print("时间线分布:")
        for month, count in monthly_data:
            bar_length = count * 2  # 每个客户用2个字符表示
            bar = "█" * bar_length
            print(f"{int(month)}月: {bar} ({count}个)")

        cursor.close()
        conn.close()

        print("\n[SUCCESS] 高风险客户详细分析完成！")

        return True

    except Exception as e:
        print(f"[ERROR] 分析失败: {e}")
        return False

def main():
    """主函数"""
    print("[INFO] 开始高风险客户详细分析...")

    if detailed_high_risk_analysis():
        print("\n[SUCCESS] 分析成功！")
        sys.exit(0)
    else:
        print("\n[ERROR] 分析失败！")
        sys.exit(1)

if __name__ == "__main__":
    main()