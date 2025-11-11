#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证高风险客户的联网核查覆盖情况
详细检查每个高风险客户的联网核查记录
"""

import mysql.connector
import sys

def verify_high_risk_verification():
    """详细验证高风险客户联网核查情况"""
    print("="*80)
    print("高风险客户联网核查覆盖情况详细验证")
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

        # 1. 查询高风险个人客户的联网核查情况
        print("\n[1] 高风险个人客户联网核查验证")
        print("-" * 60)

        cursor.execute("""
            SELECT DISTINCT
                p.Cst_no,
                p.Acc_name,
                p.Id_no,
                r.Time AS risk_assessment_time,
                COUNT(l.Id_no) AS verification_count,
                MIN(l.Date) AS first_verification_date,
                MAX(l.Date) AS last_verification_date
            FROM tb_risk_new r
            JOIN tb_cst_pers p ON r.Cst_no = p.Cst_no
            LEFT JOIN tb_lwhc_log l ON p.Id_no = l.Id_no
            WHERE r.Risk_code = '01' AND r.Acc_type = '11'
            GROUP BY p.Cst_no, p.Acc_name, p.Id_no, r.Time
            ORDER BY p.Cst_no
            LIMIT 10
        """)

        personal_results = cursor.fetchall()

        print("前10个高风险个人客户:")
        print(f"{'客户号':<10} {'客户名称':<10} {'身份证号':<20} {'风险评估时间':<8} {'核查次数':<6} {'首次核查':<8} {'最后核查':<8}")
        print("-" * 90)

        no_verification_count = 0
        for row in personal_results:
            cst_no, name, id_no, risk_time, verify_count, first_date, last_date = row
            verification_status = f"{verify_count}次" if verify_count > 0 else "无"
            first_date_str = first_date if first_date else "无"
            last_date_str = last_date if last_date else "无"
            print(f"{cst_no:<10} {name:<10} {id_no:<20} {risk_time:<8} {verification_status:<6} {first_date_str:<8} {last_date_str:<8}")

            if verify_count == 0:
                no_verification_count += 1

        print(f"\n个人高风险客户样例中无联网核查记录: {no_verification_count} 个")

        # 2. 查询高风险企业客户的联网核查情况
        print("\n[2] 高风险企业客户联网核查验证")
        print("-" * 60)

        cursor.execute("""
            SELECT DISTINCT
                u.Cst_no,
                u.Acc_name,
                u.License,
                r.Time AS risk_assessment_time,
                COUNT(l.Id_no) AS verification_count,
                MIN(l.Date) AS first_verification_date,
                MAX(l.Date) AS last_verification_date
            FROM tb_risk_new r
            JOIN tb_cst_unit u ON r.Cst_no = u.Cst_no
            LEFT JOIN tb_lwhc_log l ON u.License = l.Id_no
            WHERE r.Risk_code = '01' AND r.Acc_type = '13'
            GROUP BY u.Cst_no, u.Acc_name, u.License, r.Time
            ORDER BY u.Cst_no
        """)

        corporate_results = cursor.fetchall()

        print("所有高风险企业客户:")
        print(f"{'客户号':<10} {'企业名称':<20} {'营业执照':<20} {'风险评估时间':<8} {'核查次数':<6} {'首次核查':<8} {'最后核查':<8}")
        print("-" * 90)

        corporate_no_verification_count = 0
        for row in corporate_results:
            cst_no, name, license_no, risk_time, verify_count, first_date, last_date = row
            verification_status = f"{verify_count}次" if verify_count > 0 else "无"
            first_date_str = first_date if first_date else "无"
            last_date_str = last_date if last_date else "无"
            print(f"{cst_no:<10} {name[:20]:<20} {license_no:<20} {risk_time:<8} {verification_status:<6} {first_date_str:<8} {last_date_str:<8}")

            if verify_count == 0:
                corporate_no_verification_count += 1

        print(f"\n企业高风险客户中无联网核查记录: {corporate_no_verification_count} 个")

        # 3. 统计总体覆盖情况
        print("\n[3] 总体联网核查覆盖统计")
        print("-" * 50)

        # 计算个人客户覆盖情况
        cursor.execute("""
            SELECT
                COUNT(*) as total_high_risk_personal,
                SUM(CASE WHEN verification_count > 0 THEN 1 ELSE 0 END) as personal_with_verification
            FROM (
                SELECT
                    p.Cst_no,
                    COUNT(l.Id_no) AS verification_count
                FROM tb_risk_new r
                JOIN tb_cst_pers p ON r.Cst_no = p.Cst_no
                LEFT JOIN tb_lwhc_log l ON p.Id_no = l.Id_no
                WHERE r.Risk_code = '01' AND r.Acc_type = '11'
                GROUP BY p.Cst_no
            ) AS personal_stats
        """)

        personal_stats = cursor.fetchone()

        # 计算企业客户覆盖情况
        cursor.execute("""
            SELECT
                COUNT(*) as total_high_risk_corporate,
                SUM(CASE WHEN verification_count > 0 THEN 1 ELSE 0 END) as corporate_with_verification
            FROM (
                SELECT
                    u.Cst_no,
                    COUNT(l.Id_no) AS verification_count
                FROM tb_risk_new r
                JOIN tb_cst_unit u ON r.Cst_no = u.Cst_no
                LEFT JOIN tb_lwhc_log l ON u.License = l.Id_no
                WHERE r.Risk_code = '01' AND r.Acc_type = '13'
                GROUP BY u.Cst_no
            ) AS corporate_stats
        """)

        corporate_stats = cursor.fetchone()

        total_high_risk = personal_stats[0] + corporate_stats[0]
        total_with_verification = personal_stats[1] + corporate_stats[1]
        total_without_verification = total_high_risk - total_with_verification

        print(f"高风险个人客户总数: {personal_stats[0]}")
        print(f"高风险个人客户有联网核查: {personal_stats[1]}")
        print(f"高风险企业客户总数: {corporate_stats[0]}")
        print(f"高风险企业客户有联网核查: {corporate_stats[1]}")
        print(f"高风险客户总数: {total_high_risk}")
        print(f"有联网核查记录: {total_with_verification}")
        print(f"无联网核查记录: {total_without_verification}")

        coverage_rate = (total_with_verification / total_high_risk) * 100 if total_high_risk > 0 else 0
        print(f"联网核查覆盖率: {coverage_rate:.1f}%")

        cursor.close()
        conn.close()

        return total_without_verification

    except Exception as e:
        print(f"[ERROR] 验证失败: {e}")
        return -1

def main():
    """主函数"""
    print("[INFO] 开始详细验证高风险客户联网核查情况...")

    without_verification = verify_high_risk_verification()

    if without_verification > 0:
        print(f"\n[RESULT] 发现 {without_verification} 个高风险客户无联网核查记录，需要补充！")
        sys.exit(1)
    elif without_verification == 0:
        print(f"\n[RESULT] 所有高风险客户都有联网核查记录，覆盖完整！")
        sys.exit(0)
    else:
        print(f"\n[ERROR] 验证过程中发生错误")
        sys.exit(2)

if __name__ == "__main__":
    main()