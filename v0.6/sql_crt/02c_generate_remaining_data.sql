-- =====================================================
-- AML300数据库剩余数据生成脚本
-- 包含大额交易报告、可疑交易报告、跨境交易等
-- 创建时间: 2025-11-11
-- 版本: v1.0
-- =====================================================

USE AML300;

-- =====================================================
-- 8. 大额交易报告生成 (约300份)
-- =====================================================
-- 为大额交易生成报告
INSERT INTO tb_lar_report (
    RPMN, Report_Date, Institution_Code, Customer_ID, Customer_Name,
    Customer_Type, Account_No, Account_Name, Transaction_Date,
    Transaction_Time, Reporting_Feature_Code, Report_Amount, Currency,
    Transaction_Type, Counterpart_Account, Counterpart_Name, Counterpart_Bank
)
SELECT
    CONCAT('LAR', LPAD(seq, 10, '0')) as RPMN,
    CONCAT(
        '2020',
        LPAD(FLOOR(RAND() * 6) + 1, 2, '0'),
        LPAD(FLOOR(RAND() * 28) + 1, 2, '0')
    ) as Report_Date,
    '104100000019' as Institution_Code,
    t.Cst_no as Customer_ID,
    COALESCE(p.Acc_name, u.Acc_name) as Customer_Name,
    CASE WHEN p.Cst_no IS NOT NULL THEN '11' ELSE '13' END as Customer_Type,
    t.Self_acc_no as Account_No,
    t.Org_amt as Account_Name,
    t.Date as Transaction_Date,
    t.Tstm as Transaction_Time,
    CASE
        WHEN t.Org_amt >= 50000 AND t.Cur = 'CNY' THEN '0501' -- 现金交易≥5万元
        WHEN t.Org_amt >= 2000000 AND t.Cur = 'CNY' THEN '0502' -- 非自然人转账≥200万元
        WHEN t.Org_amt >= 500000 AND t.Cur = 'CNY' THEN '0503' -- 自然人境内转账≥50万元
        WHEN t.Org_amt >= 200000 AND t.Cur = 'CNY' THEN '0504' -- 自然人跨境转账≥20万元
        ELSE '0501'
    END as Reporting_Feature_Code,
    t.Org_amt as Report_Amount,
    t.Cur as Currency,
    t.Txn_type as Transaction_Type,
    t.Opp_acc as Counterpart_Account,
    t.Opp_name as Counterpart_Name,
    CASE FLOOR(RAND() * 10)
        WHEN 0 THEN '中国工商银行'
        WHEN 1 THEN '中国建设银行'
        WHEN 2 THEN '中国银行'
        WHEN 3 THEN '中国农业银行'
        WHEN 4 THEN '交通银行'
        WHEN 5 THEN '招商银行'
        WHEN 6 THEN '中信银行'
        WHEN 7 THEN '光大银行'
        WHEN 8 THEN '民生银行'
        ELSE '平安银行'
    END as Counterpart_Bank
FROM tb_acc_txn t
LEFT JOIN tb_cst_pers p ON t.Cst_no = p.Cst_no
LEFT JOIN tb_cst_unit u ON t.Cst_no = u.Cst_no
WHERE
    (t.Org_amt >= 50000 AND t.Cur = 'CNY') OR  -- 大额现金交易
    (t.Org_amt >= 200000 AND t.Cur = 'CNY')   -- 大额转账
ORDER BY t.Org_amt DESC
LIMIT 300;

-- =====================================================
-- 9. 可疑交易报告生成 (约150份)
-- =====================================================
INSERT INTO tb_sus_report (
    SNO, Report_Date, Institution_Code, Customer_ID, Customer_Name,
    Customer_Type, Account_No, Account_Name, Suspicious_Feature_Code,
    Transaction_Date, Transaction_Amount, Currency, Suspicious_Reason, Report_Content
)
SELECT
    CONCAT('SUS', LPAD(seq, 10, '0')) as SNO,
    CONCAT(
        '2020',
        LPAD(FLOOR(RAND() * 6) + 1, 2, '0'),
        LPAD(FLOOR(RAND() * 28) + 1, 2, '0')
    ) as Report_Date,
    '104100000019' as Institution_Code,
    r.Cst_no as Customer_ID,
    COALESCE(p.Acc_name, u.Acc_name) as Customer_Name,
    CASE WHEN p.Cst_no IS NOT NULL THEN '11' ELSE '13' END as Customer_Type,
    a.Self_acc_no as Account_No,
    a.Acc_name as Account_Name,
    CASE FLOOR(RAND() * 20)
        WHEN 0 THEN '0101' -- 分散转入集中转出
        WHEN 1 THEN '0102' -- 集中转入分散转出
        WHEN 2 THEN '0103' -- 资金快进快出
        WHEN 3 THEN '0104' -- 过夜资金异常
        WHEN 4 THEN '0105' -- 交易金额异常
        WHEN 5 THEN '0106' -- 交易频率异常
        WHEN 6 THEN '0107' -- 交易时间异常
        WHEN 7 THEN '0108' -- 跨境交易异常
        WHEN 8 THEN '0109' -- 现金交易异常
        WHEN 9 THEN '0110' -- 第三方支付异常
        ELSE '0101'
    END as Suspicious_Feature_Code,
    CONCAT(
        '2020',
        LPAD(FLOOR(RAND() * 6) + 1, 2, '0'),
        LPAD(FLOOR(RAND() * 28) + 1, 2, '0')
    ) as Transaction_Date,
    ROUND(RAND() * 1000000 + 50000, 2) as Transaction_Amount,
    'CNY' as Currency,
    CASE FLOOR(RAND() * 8)
        WHEN 0 THEN '客户交易模式异常，资金快进快出，可能存在洗钱嫌疑'
        WHEN 1 THEN '频繁进行大额现金交易，与客户身份和经济状况不符'
        WHEN 2 THEN '跨境交易资金来源不明，存在地下钱庄嫌疑'
        WHEN 3 THEN '与敏感地区人员有资金往来，可能涉及非法活动'
        WHEN 4 THEN '交易时间集中在非工作时段，交易行为异常'
        WHEN 5 THEN '多个账户关联交易，存在团伙作案嫌疑'
        WHEN 6 THEN '资金流动与企业经营不符，存在虚开发票嫌疑'
        ELSE '客户身份背景复杂，需要进一步调查'
    END as Suspicious_Reason,
    CASE FLOOR(RAND() * 5)
        WHEN 0 THEN '客户交易行为异常，建议列入重点监控名单'
        WHEN 1 THEN '涉及可疑资金流动，建议加强尽职调查'
        WHEN 2 THEN '交易模式符合洗钱特征，建议报告监管部门'
        WHEN 3 THEN '客户风险等级较高，建议限制部分业务'
        ELSE '需要进一步核实客户身份和资金来源'
    END as Report_Content
FROM tb_risk_new r
JOIN tb_acc a ON r.Cst_no = a.Cst_no
LEFT JOIN tb_cst_pers p ON r.Cst_no = p.Cst_no
LEFT JOIN tb_cst_unit u ON r.Cst_no = u.Cst_no
WHERE r.Risk_code IN ('01', '02')  -- 高风险和中高风险客户
LIMIT 150;

-- =====================================================
-- 10. 跨境交易数据生成 (约500笔)
-- =====================================================
INSERT INTO tb_cross_border (
    Cst_no, Self_acc_no, Date, Cur, Org_amt, Rmb_amt, Exchange_rate,
    Part_nation, Part_name, Part_account, Part_bank, Purpose, Txn_type
)
SELECT
    t.Cst_no,
    t.Self_acc_no,
    t.Date,
    CASE FLOOR(RAND() * 8)
        WHEN 0 THEN 'USD'
        WHEN 1 THEN 'EUR'
        WHEN 2 THEN 'GBP'
        WHEN 3 THEN 'JPY'
        WHEN 4 THEN 'HKD'
        WHEN 5 THEN 'SGD'
        WHEN 6 THEN 'AUD'
        ELSE 'CAD'
    END as Cur,
    ROUND(RAND() * 50000 + 1000, 2) as Org_amt,
    ROUND((RAND() * 50000 + 1000) * CASE FLOOR(RAND() * 8)
        WHEN 0 THEN 7.0  -- USD
        WHEN 1 THEN 8.0  -- EUR
        WHEN 2 THEN 9.0  -- GBP
        WHEN 3 THEN 0.06 -- JPY
        WHEN 4 THEN 0.9  -- HKD
        WHEN 5 THEN 5.0  -- SGD
        WHEN 6 THEN 5.0  -- AUD
        ELSE 5.5        -- CAD
    END, 2) as Rmb_amt,
    CASE FLOOR(RAND() * 8)
        WHEN 0 THEN 7.0
        WHEN 1 THEN 8.0
        WHEN 2 THEN 9.0
        WHEN 3 THEN 0.06
        WHEN 4 THEN 0.9
        WHEN 5 THEN 5.0
        WHEN 6 THEN 5.0
        ELSE 5.5
    END as Exchange_rate,
    CASE FLOOR(RAND() * 20)
        WHEN 0 THEN '840' -- 美国
        WHEN 1 THEN '826' -- 英国
        WHEN 2 THEN '124' -- 加拿大
        WHEN 3 THEN '392' -- 日本
        WHEN 4 THEN '344' -- 香港
        WHEN 5 THEN '702' -- 新加坡
        WHEN 6 THEN '036' -- 澳大利亚
        WHEN 7 THEN '250' -- 法国
        WHEN 8 THEN '276' -- 德国
        WHEN 9 THEN '380' -- 意大利
        WHEN 10 THEN '528' -- 荷兰
        WHEN 11 THEN '756' -- 瑞士
        WHEN 12 THEN '704' -- 越南
        WHEN 13 THEN '418' -- 韩国
        WHEN 14 THEN '458' -- 马来西亚
        WHEN 15 THEN '608' -- 菲律宾
        WHEN 16 THEN '764' -- 泰国
        WHEN 17 THEN '360' -- 印度尼西亚
        WHEN 18 THEN '156' -- 中国台湾
        ELSE '344' -- 香港
    END as Part_nation,
    CASE FLOOR(RAND() * 20)
        WHEN 0 THEN 'ABC Company'
        WHEN 1 THEN 'XYZ Trading'
        WHEN 2 THEN 'Global Tech'
        WHEN 3 THEN 'International Corp'
        WHEN 4 THEN 'Overseas Investment'
        WHEN 5 THEN 'Foreign Trade Co'
        WHEN 6 THEN 'Export Import Ltd'
        WHEN 7 THEN 'World Business'
        WHEN 8 THEN 'Global Services'
        WHEN 9 THEN 'International Trading'
        WHEN 10 THEN 'Overseas Partners'
        WHEN 11 THEN 'Foreign Investments'
        WHEN 12 THEN 'Global Solutions'
        WHEN 13 THEN 'World Trade Center'
        WHEN 14 THEN 'International Holdings'
        WHEN 15 THEN 'Overseas Enterprises'
        WHEN 16 THEN 'Foreign Corporation'
        WHEN 17 THEN 'Global Alliance'
        WHEN 18 THEN 'World Commerce'
        ELSE 'International Group'
    END as Part_name,
    CONCAT('INTL', LPAD(FLOOR(RAND() * 1000000), 6, '0')) as Part_account,
    CASE FLOOR(RAND() * 10)
        WHEN 0 THEN 'Citibank'
        WHEN 1 THEN 'HSBC'
        WHEN 2 THEN 'Standard Chartered'
        WHEN 3 THEN 'Bank of America'
        WHEN 4 THEN 'JPMorgan Chase'
        WHEN 5 THEN 'Wells Fargo'
        WHEN 6 THEN 'Deutsche Bank'
        WHEN 7 THEN 'BNP Paribas'
        WHEN 8 THEN 'UBS'
        ELSE 'Credit Suisse'
    END as Part_bank,
    CASE FLOOR(RAND() * 8)
        WHEN 0 THEN '货物贸易'
        WHEN 1 THEN '服务贸易'
        WHEN 2 THEN '投资收益'
        WHEN 3 THEN '个人汇款'
        WHEN 4 THEN '经常转移'
        WHEN 5 THEN '资本项目'
        WHEN 6 THEN '直接投资'
        ELSE '证券投资'
    END as Purpose,
    CASE FLOOR(RAND() * 5)
        WHEN 0 THEN '汇出'
        WHEN 1 THEN '汇入'
        WHEN 2 THEN '汇出'
        WHEN 3 THEN '汇入'
        ELSE '汇出'
    END as Txn_type
FROM tb_acc_txn t
WHERE t.Date LIKE '2020%'
AND t.Org_amt >= 10000  -- 只为较大交易生成跨境记录
ORDER BY RAND()
LIMIT 500;

-- =====================================================
-- 11. 客户身份信息变更记录 (约200条)
-- =====================================================
INSERT INTO tb_identity_change (
    Cst_no, Change_Date, Change_Type, Old_Value, New_Value, Reason, Approver
)
SELECT
    Cst_no,
    CONCAT(
        '2020',
        LPAD(FLOOR(RAND() * 6) + 1, 2, '0'),
        LPAD(FLOOR(RAND() * 28) + 1, 2, '0')
    ) as Change_Date,
    CASE FLOOR(RAND() * 5)
        WHEN 0 THEN '联系电话'
        WHEN 1 THEN '居住地址'
        WHEN 2 THEN '工作单位'
        WHEN 3 THEN '电子邮箱'
        ELSE '手机号码'
    END as Change_Type,
    CASE FLOOR(RAND() * 5)
        WHEN 0 THEN '13800138000'
        WHEN 1 THEN '北京市朝阳区旧地址100号'
        WHEN 2 THEN '旧工作单位'
        WHEN 3 THEN 'old@example.com'
        ELSE '13900139000'
    END as Old_Value,
    CASE FLOOR(RAND() * 5)
        WHEN 0 THEN CONCAT('138', LPAD(FLOOR(RAND() * 100000000), 8, '0'))
        WHEN 1 THEN '北京市朝阳区新地址' || FLOOR(RAND() * 1000 + 1) || '号'
        WHEN 2 THEN '新工作单位'
        WHEN 3 THEN 'new@example.com'
        ELSE CONCAT('139', LPAD(FLOOR(RAND() * 100000000), 8, '0'))
    END as New_Value,
    CASE FLOOR(RAND() * 5)
        WHEN 0 THEN '客户主动申请更新'
        WHEN 1 THEN '联系方式变更'
        WHEN 2 THEN '地址搬迁'
        WHEN 3 THEN '工作变动'
        ELSE '信息纠错'
    END as Reason,
    CASE FLOOR(RAND() * 5)
        WHEN 0 THEN '张经理'
        WHEN 1 THEN '李主管'
        WHEN 2 THEN '王主任'
        WHEN 3 THEN '刘总监'
        ELSE '陈审批'
    END as Approver
FROM (
    SELECT Cst_no FROM tb_cst_pers UNION SELECT Cst_no FROM tb_cst_unit
) temp
LIMIT 200;

-- =====================================================
-- 12. 洗钱风险评估记录 (约300条)
-- =====================================================
INSERT INTO tb_aml_risk_assessment (
    Cst_no, Assessment_Date, Risk_Level, Risk_Score, Assessment_Factors,
    Assessment_Result, Assessor, Review_Date, Reviewer
)
SELECT
    Cst_no,
    CONCAT(
        '2020',
        LPAD(FLOOR(RAND() * 6) + 1, 2, '0'),
        LPAD(FLOOR(RAND() * 28) + 1, 2, '0')
    ) as Assessment_Date,
    CASE FLOOR(RAND() * 10)
        WHEN 0 THEN '01' -- 高风险
        WHEN 1 THEN '01' -- 高风险
        WHEN 2 THEN '02' -- 中高风险
        WHEN 3 THEN '02' -- 中高风险
        WHEN 4 THEN '03' -- 中风险
        WHEN 5 THEN '03' -- 中风险
        WHEN 6 THEN '03' -- 中风险
        WHEN 7 THEN '03' -- 中风险
        WHEN 8 THEN '04' -- 低风险
        ELSE '04' -- 低风险
    END as Risk_Level,
    CASE FLOOR(RAND() * 10)
        WHEN 0 THEN 85 + FLOOR(RAND() * 15)
        WHEN 1 THEN 85 + FLOOR(RAND() * 15)
        WHEN 2 THEN 65 + FLOOR(RAND() * 15)
        WHEN 3 THEN 65 + FLOOR(RAND() * 15)
        WHEN 4 THEN 45 + FLOOR(RAND() * 15)
        WHEN 5 THEN 45 + FLOOR(RAND() * 15)
        WHEN 6 THEN 45 + FLOOR(RAND() * 15)
        WHEN 7 THEN 45 + FLOOR(RAND() * 15)
        WHEN 8 THEN 15 + FLOOR(RAND() * 15)
        ELSE 15 + FLOOR(RAND() * 15)
    END as Risk_Score,
    CASE FLOOR(RAND() * 8)
        WHEN 0 THEN '客户身份、地域、行业、交易行为'
        WHEN 1 THEN '交易金额、频率、对手信息'
        WHEN 2 THEN '资金来源、用途、流向'
        WHEN 3 THEN '客户背景、关联关系'
        WHEN 4 THEN '历史交易模式、异常行为'
        WHEN 5 THEN '行业风险、地理位置风险'
        WHEN 6 THEN '产品服务风险、渠道风险'
        ELSE '综合评估因子'
    END as Assessment_Factors,
    CASE FLOOR(RAND() * 8)
        WHEN 0 THEN '高风险客户，建议加强监控'
        WHEN 1 THEN '高风险客户，建议限制业务'
        WHEN 2 THEN '中高风险客户，建议定期评估'
        WHEN 3 THEN '中高风险客户，需要关注'
        WHEN 4 THEN '中等风险客户，正常监控'
        WHEN 5 THEN '中等风险客户，定期检查'
        WHEN 6 THEN '低风险客户，简化程序'
        ELSE '低风险客户，正常服务'
    END as Assessment_Result,
    CASE FLOOR(RAND() * 5)
        WHEN 0 THEN '王评估师'
        WHEN 1 THEN '李分析师'
        WHEN 2 THEN '张专员'
        WHEN 3 THEN '刘审查员'
        ELSE '陈风控'
    END as Assessor,
    CONCAT(
        '2020',
        LPAD(FLOOR(RAND() * 6) + 1, 2, '0'),
        LPAD(FLOOR(RAND() * 28) + 1, 2, '0')
    ) as Review_Date,
    CASE FLOOR(RAND() * 5)
        WHEN 0 THEN '赵复核'
        WHEN 1 THEN '钱审核'
        WHEN 2 THEN '孙检查'
        WHEN 3 THEN '李验证'
        ELSE '周确认'
    END as Reviewer
FROM (
    SELECT Cst_no FROM tb_cst_pers UNION SELECT Cst_no FROM tb_cst_unit
) temp
LIMIT 300;

-- =====================================================
-- 13. 政治公众人物信息 (约50条)
-- =====================================================
INSERT INTO tb_pep_info (
    Cst_no, Pep_Type, Pep_Name, Pep_Position, Pep_Country,
    Pep_Organization, Relationship, Start_Date, End_Date, Status
)
SELECT
    Cst_no,
    CASE FLOOR(RAND() * 4)
        WHEN 0 THEN '01' -- 国内政要
        WHEN 1 THEN '02' -- 外国政要
        WHEN 2 THEN '03' -- 国际组织高管
        ELSE '04' -- 政要亲属
    END as Pep_Type,
    CASE FLOOR(RAND() * 20)
        WHEN 0 THEN '某部长'
        WHEN 1 THEN '某省长'
        WHEN 2 THEN '某市长'
        WHEN 3 THEN '某局长'
        WHEN 4 THEN '某主任'
        WHEN 5 THEN '某书记'
        WHEN 6 THEN '某主席'
        WHEN 7 THEN '某董事长'
        WHEN 8 THEN '某行长'
        WHEN 9 THEN '某总裁'
        WHEN 10 THEN '某总经理'
        WHEN 11 THEN '某秘书长'
        WHEN 12 THEN '某委员长'
        WHEN 13 THEN '某大使'
        WHEN 14 THEN '某代表'
        WHEN 15 THEN '某官员'
        WHEN 16 THEN '某领导'
        WHEN 17 THEN '某主管'
        WHEN 18 THEN '某负责人'
        ELSE '某管理者'
    END as Pep_Name,
    CASE FLOOR(RAND() * 10)
        WHEN 0 THEN '部长'
        WHEN 1 THEN '省长'
        WHEN 2 THEN '市长'
        WHEN 3 THEN '局长'
        WHEN 4 THEN '主任'
        WHEN 5 THEN '书记'
        WHEN 6 THEN '主席'
        WHEN 7 THEN '董事长'
        WHEN 8 THEN '行长'
        ELSE '总裁'
    END as Pep_Position,
    CASE FLOOR(RAND() * 10)
        WHEN 0 THEN '156' -- 中国
        WHEN 1 THEN '840' -- 美国
        WHEN 2 THEN '826' -- 英国
        WHEN 3 THEN '250' -- 法国
        WHEN 4 THEN '276' -- 德国
        WHEN 5 THEN '380' -- 意大利
        WHEN 6 THEN '124' -- 加拿大
        WHEN 7 THEN '036' -- 澳大利亚
        WHEN 8 THEN '392' -- 日本
        ELSE '410' -- 韩国
    END as Pep_Country,
    CASE FLOOR(RAND() * 10)
        WHEN 0 THEN '政府部门'
        WHEN 1 THEN '国有企业'
        WHEN 2 THEN '金融机构'
        WHEN 3 THEN '国际组织'
        WHEN 4 THEN '监管机构'
        WHEN 5 THEN '立法机构'
        WHEN 6 THEN '司法机构'
        WHEN 7 THEN '执政党'
        WHEN 8 THEN '军队'
        ELSE '外交机构'
    END as Pep_Organization,
    CASE FLOOR(RAND() * 5)
        WHEN 0 THEN '本人'
        WHEN 1 THEN '配偶'
        WHEN 2 THEN '子女'
        WHEN 3 THEN '父母'
        ELSE '其他亲属'
    END as Relationship,
    '20200101' as Start_Date,
    '20251231' as End_Date,
    '01' as Status -- 在职
FROM (
    SELECT Cst_no FROM tb_cst_pers UNION SELECT Cst_no FROM tb_cst_unit
) temp
WHERE RAND() > 0.95 -- 约5%的客户有PEP信息
LIMIT 50;

-- =====================================================
-- 14. 制裁名单筛查记录 (约100条)
-- =====================================================
INSERT INTO tb_sanctions_screening (
    Cst_no, Screen_Date, Screen_Result, Matched_List, Match_Name,
    Match_Score, Review_Result, Reviewer
)
SELECT
    Cst_no,
    CONCAT(
        '2020',
        LPAD(FLOOR(RAND() * 6) + 1, 2, '0'),
        LPAD(FLOOR(RAND() * 28) + 1, 2, '0')
    ) as Screen_Date,
    CASE FLOOR(RAND() * 100)
        WHEN 0 THEN '01' -- 完全匹配
        WHEN 1 THEN '02' -- 高度相似
        WHEN 2 THEN '03' -- 可能匹配
        WHEN 3 THEN '03' -- 可能匹配
        ELSE '00' -- 无匹配
    END as Screen_Result,
    CASE FLOOR(RAND() * 10)
        WHEN 0 THEN 'OFAC制裁名单'
        WHEN 1 THEN '联合国制裁名单'
        WHEN 2 THEN '欧盟制裁名单'
        WHEN 3 THEN '英国制裁名单'
        WHEN 4 THEN '澳大利亚制裁名单'
        WHEN 5 THEN '加拿大制裁名单'
        WHEN 6 THEN '瑞士制裁名单'
        WHEN 7 THEN '日本制裁名单'
        WHEN 8 THEN '新加坡制裁名单'
        ELSE '香港制裁名单'
    END as Matched_List,
    CASE FLOOR(RAND() * 20)
        WHEN 0 THEN 'Target Person 1'
        WHEN 1 THEN 'Target Entity 1'
        WHEN 2 THEN 'Suspicious Individual'
        WHEN 3 THEN 'Blocked Company'
        WHEN 4 THEN 'Sanctioned Person'
        WHEN 5 THEN 'Prohibited Entity'
        WHEN 6 THEN 'Designated Individual'
        WHEN 7 THEN 'Restricted Organization'
        WHEN 8 THEN 'Watchlisted Person'
        WHEN 9 THEN 'Concerned Entity'
        WHEN 10 THEN 'Monitored Individual'
        WHEN 11 THEN 'Flagged Company'
        WHEN 12 THEN 'Alert Person'
        WHEN 13 THEN 'Watch List Entry'
        WHEN 14 THEN 'Sanction Target'
        WHEN 15 THEN 'Blocked Person'
        WHEN 16 THEN 'Prohibited Entity'
        WHEN 17 THEN 'Designated Target'
        WHEN 18 THEN 'Restrict Entity'
        ELSE 'Monitored Person'
    END as Match_Name,
    CASE FLOOR(RAND() * 40) + 60 as Match_Score, -- 60-100分
    CASE FLOOR(RAND() * 5)
        WHEN 0 THEN '确认无风险'
        WHEN 1 THEN '需要进一步调查'
        WHEN 2 THEN '误报，可以解除'
        WHEN 3 THEN '相似但非同一人'
        ELSE '确认匹配，需要采取措施'
    END as Review_Result,
    CASE FLOOR(RAND() * 5)
        WHEN 0 THEN '合规专员A'
        WHEN 1 THEN '风控经理B'
        WHEN 2 THEN '合规主管C'
        WHEN 3 THEN '审查员D'
        ELSE '风控专员E'
    END as Reviewer
FROM (
    SELECT Cst_no FROM tb_cst_pers UNION SELECT Cst_no FROM tb_cst_unit
) temp
WHERE RAND() > 0.9 -- 约10%的客户需要筛查
LIMIT 100;

-- =====================================================
-- 15. 客户尽职调查记录 (约200条)
-- =====================================================
INSERT INTO tb_cdd_edd_record (
    Cst_no, Investigation_Date, Investigation_Type, Risk_Level,
    Investigation_Content, Investigation_Result, Investigator,
    Approval_Date, Approver
)
SELECT
    Cst_no,
    CONCAT(
        '2020',
        LPAD(FLOOR(RAND() * 6) + 1, 2, '0'),
        LPAD(FLOOR(RAND() * 28) + 1, 2, '0')
    ) as Investigation_Date,
    CASE FLOOR(RAND() * 3)
        WHEN 0 THEN 'CDD' -- 客户尽职调查
        WHEN 1 THEN 'EDD' -- 增强尽职调查
        ELSE 'CDD'
    END as Investigation_Type,
    CASE FLOOR(RAND() * 4)
        WHEN 0 THEN '01' -- 高风险
        WHEN 1 THEN '02' -- 中高风险
        WHEN 2 THEN '03' -- 中风险
        ELSE '04' -- 低风险
    END as Risk_Level,
    CASE FLOOR(RAND() * 8)
        WHEN 0 THEN '客户身份核实、资金来源调查、交易目的确认'
        WHEN 1 THEN '企业背景调查、实际控制人识别、经营状况评估'
        WHEN 2 THEN '个人资信调查、收入来源核实、投资经验评估'
        WHEN 3 THEN '关联关系分析、异常交易识别、风险因素评估'
        WHEN 4 THEN '合规检查、黑名单筛查、制裁名单核实'
        WHEN 5 THEN '客户身份识别、受益所有人调查、持股结构分析'
        WHEN 6 THEN '业务背景调查、交易模式分析、资金流向追踪'
        ELSE '全面风险评估、持续监控计划、定期审查安排'
    END as Investigation_Content,
    CASE FLOOR(RAND() * 8)
        WHEN 0 THEN '调查通过，可以建立业务关系'
        WHEN 1 THEN '需要补充材料，进一步核实'
        WHEN 2 THEN '发现风险点，建议加强监控'
        WHEN 3 THEN '调查完成，风险可控'
        WHEN 4 THEN '需要持续关注，定期复查'
        WHEN 5 THEN '基本符合要求，建立正常业务关系'
        WHEN 6 THEN '存在疑点，建议暂缓业务'
        ELSE '调查通过，纳入正常客户管理'
    END as Investigation_Result,
    CASE FLOOR(RAND() * 5)
        WHEN 0 THEN '调查员A'
        WHEN 1 THEN '调查员B'
        WHEN 2 THEN '调查员C'
        WHEN 3 THEN '调查员D'
        ELSE '调查员E'
    END as Investigator,
    CONCAT(
        '2020',
        LPAD(FLOOR(RAND() * 6) + 1, 2, '0'),
        LPAD(FLOOR(RAND() * 28) + 1, 2, '0')
    ) as Approval_Date,
    CASE FLOOR(RAND() * 5)
        WHEN 0 THEN '审批人A'
        WHEN 1 THEN '审批人B'
        WHEN 2 THEN '审批人C'
        WHEN 3 THEN '审批人D'
        ELSE '审批人E'
    END as Approver
FROM (
    SELECT Cst_no FROM tb_cst_pers UNION SELECT Cst_no FROM tb_cst_unit
) temp
LIMIT 200;

-- 重新启用外键检查
SET FOREIGN_KEY_CHECKS = 1;

-- =====================================================
-- 数据生成完成统计
-- =====================================================
SELECT '=== AML300数据库数据生成完成统计 ===' AS Message;
SELECT CONCAT('个人客户数量: ', COUNT(*)) AS Message FROM tb_cst_pers;
SELECT CONCAT('企业客户数量: ', COUNT(*)) AS Message FROM tb_cst_unit;
SELECT CONCAT('银行网点数量: ', COUNT(*)) AS Message FROM tb_bank;
SELECT CONCAT('账户数量: ', COUNT(*)) AS Message FROM tb_acc;
SELECT CONCAT('交易记录数量: ', COUNT(*)) AS Message FROM tb_acc_txn;
SELECT CONCAT('风险评估记录数量: ', COUNT(*)) AS Message FROM tb_risk_new;
SELECT CONCAT('大额交易报告数量: ', COUNT(*)) AS Message FROM tb_lar_report;
SELECT CONCAT('可疑交易报告数量: ', COUNT(*)) AS Message FROM tb_sus_report;
SELECT CONCAT('跨境交易记录数量: ', COUNT(*)) AS Message FROM tb_cross_border;
SELECT CONCAT('联网核查记录数量: ', COUNT(*)) AS Message FROM tb_lwhc_log;
SELECT CONCAT('身份变更记录数量: ', COUNT(*)) AS Message FROM tb_identity_change;
SELECT CONCAT('AML评估记录数量: ', COUNT(*)) AS Message FROM tb_aml_risk_assessment;
SELECT CONCAT('PEP信息记录数量: ', COUNT(*)) AS Message FROM tb_pep_info;
SELECT CONCAT('制裁筛查记录数量: ', COUNT(*)) AS Message FROM tb_sanctions_screening;
SELECT CONCAT('尽职调查记录数量: ', COUNT(*)) AS Message FROM tb_cdd_edd_record;

SELECT '所有数据生成完成！请运行数据验证脚本检查数据质量。' AS Message;