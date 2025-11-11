#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分析高风险客户的联网核查情况
检查哪些高风险客户没有联网核查记录
"""

import mysql.connector
import sys

def analyze_high_risk_network_verification():
    """分析高风险客户的联网核查情况"""
    print("="*80)
    print("高风险客户联网核查情况分析")
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

        # 1. 获取所有高风险客户
        print("\n[1] 高风险客户总数统计")
        print("-" * 40)

        cursor.execute("SELECT COUNT(*) FROM tb_risk_new WHERE Risk_code = '01'")
        total_high_risk = cursor.fetchone()[0]
        print(f"高风险客户总数: {total_high_risk}")

        # 2. 查询高风险客户的联网核查情况
        print("\n[2] 高风险客户联网核查覆盖情况")
        print("-" * 50)

        # 使用更简单的查询方式避免字符集冲突
        cursor.execute("""
            SELECT DISTINCT r.Cst_no
            FROM tb_risk_new r
            WHERE r.Risk_code = '01'
            AND r.Acc_type = '11'
        """)

        high_risk_personal = [row[0] for row in cursor.fetchall()]

        cursor.execute("""
            SELECT DISTINCT r.Cst_no
            FROM tb_risk_new r
            WHERE r.Risk_code = '01'
            AND r.Acc_type = '13'
        """)

        high_risk_corporate = [row[0] for row in cursor.fetchall()]

        print(f"高风险个人客户数: {len(high_risk_personal)}")
        print(f"高风险企业客户数: {len(high_risk_corporate)}")

        # 3. 检查联网核查覆盖情况
        print("\n[3] 联网核查覆盖分析")
        print("-" * 50)

        # 检查个人客户
        cursor.execute("""
            SELECT COUNT(DISTINCT p.Cst_no)
            FROM tb_risk_new r
            JOIN tb_cst_pers p ON r.Cst_no = p.Cst_no
            LEFT JOIN tb_lwhc_log l ON p.Id_no = l.Id_no
            WHERE r.Risk_code = '01' AND r.Acc_type = '11'
        """)

        personal_with_verification = cursor.fetchone()[0]

        # 检查企业客户
        cursor.execute("""
            SELECT COUNT(DISTINCT u.Cst_no)
            FROM tb_risk_new r
            JOIN tb_cst_unit u ON r.Cst_no = u.Cst_no
            LEFT JOIN tb_lwhc_log l ON u.License = l.Id_no
            WHERE r.Risk_code = '01' AND r.Acc_type = '13'
        """)

        corporate_with_verification = cursor.fetchone()[0]

        personal_without_verification = len(high_risk_personal) - personal_with_verification
        corporate_without_verification = len(high_risk_corporate) - corporate_with_verification

        print(f"个人高风险客户有联网核查记录: {personal_with_verification}")
        print(f"个人高风险客户无联网核查记录: {personal_without_verification}")
        print(f"企业高风险客户有联网核查记录: {corporate_with_verification}")
        print(f"企业高风险客户无联网核查记录: {corporate_without_verification}")

        total_without_verification = personal_without_verification + corporate_without_verification
        print(f"\n总计无联网核查记录的高风险客户: {total_without_verification}")

        # 4. 查看当前联网核查的时间范围
        print("\n[4] 当前联网核查时间范围")
        print("-" * 40)

        cursor.execute("SELECT MIN(Date), MAX(Date), COUNT(*) FROM tb_lwhc_log")
        lwhc_time_range = cursor.fetchone()
        print(f"联网核查时间范围: {lwhc_time_range[0]} 至 {lwhc_time_range[1]}")
        print(f"联网核查总记录数: {lwhc_time_range[2]}")

        # 5. 列出需要添加联网核查记录的高风险客户样例
        if personal_without_verification > 0:
            print(f"\n[5] 需要添加联网核查记录的个人高风险客户样例（前10个）")
            print("-" * 80)

            cursor.execute("""
                SELECT p.Cst_no, p.Acc_name, p.Id_no, r.Time, r.Norm
                FROM tb_risk_new r
                JOIN tb_cst_pers p ON r.Cst_no = p.Cst_no
                WHERE r.Risk_code = '01' AND r.Acc_type = '11'
                ORDER BY r.Time
                LIMIT 10
            """)

            sample_personal = cursor.fetchall()
            print(f"{'客户号':<10} {'客户名称':<10} {'身份证号':<20} {'风险评估时间':<8} {'评估说明'}")
            print("-" * 80)

            for customer in sample_personal:
                print(f"{customer[0]:<10} {customer[1]:<10} {customer[2]:<20} {customer[3]:<8} {customer[4][:30]}")

        if corporate_without_verification > 0:
            print(f"\n[6] 需要添加联网核查记录的企业高风险客户样例（前5个）")
            print("-" * 80)

            cursor.execute("""
                SELECT u.Cst_no, u.Acc_name, u.License, r.Time, r.Norm
                FROM tb_risk_new r
                JOIN tb_cst_unit u ON r.Cst_no = u.Cst_no
                WHERE r.Risk_code = '01' AND r.Acc_type = '13'
                ORDER BY r.Time
                LIMIT 5
            """)

            sample_corporate = cursor.fetchall()
            print(f"{'客户号':<10} {'企业名称':<20} {'营业执照':<20} {'风险评估时间':<8} {'评估说明'}")
            print("-" * 80)

            for customer in sample_corporate:
                print(f"{customer[0]:<10} {customer[1]:<20} {customer[2]:<20} {customer[3]:<8} {customer[4][:30]}")

        cursor.close()
        conn.close()

        # 7. 结论和建议
        print("\n[7] 分析结论")
        print("-" * 50)
        print(f"✓ 高风险客户总数: {total_high_risk}")
        print(f"✗ 无联网核查记录的高风险客户: {total_without_verification}")
        print(f"✓ 联网核查缺失率: {(total_without_verification/total_high_risk)*100:.1f}%")

        if total_without_verification > 0:
            print(f"\n[建议] 需要为 {total_without_verification} 个高风险客户生成联网核查记录")
            print(f"[建议] 联网核查时间应设定在2020年1月1日至2020年6月1日期间")
            print(f"[建议] 应根据风险评估时间合理分配联网核查的时间点")

        return total_without_verification > 0

    except Exception as e:
        print(f"[ERROR] 分析失败: {e}")
        return False

def main():
    """主函数"""
    print("[INFO] 开始分析高风险客户联网核查情况...")

    need_fix = analyze_high_risk_network_verification()

    if need_fix:
        print("\n[RESULT] 发现高风险客户缺少联网核查记录，需要修复！")
        sys.exit(1)
    else:
        print("\n[RESULT] 高风险客户联网核查覆盖正常")
        sys.exit(0)

if __name__ == "__main__":
    main()