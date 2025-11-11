-- ===============================================================
-- 高风险客户查询SQL
-- 查询所有被认定为高风险（代码：01）的客户信息
-- 适用于反洗钱监测分析和风险管控
-- ===============================================================

-- 1. 查询个人高风险客户基本信息
SELECT
    p.Cst_no AS 客户号,
    p.Acc_name AS 客户名称,
    p.Id_no AS 证件号码,
    p.Id_type AS 证件类型,
    p.Cst_sex AS 性别,
    p.Nation AS 国籍,
    p.Contact1 AS 联系方式,
    p.Occupation AS 职业,
    p.Income AS 年收入,
    p.Address1 as 地址,
    p.Company AS 工作单位,
    r.Risk_code AS 风险等级代码,
    '高风险' AS 风险等级描述,
    r.Time AS 风险评估时间,
    r.Norm AS 评估说明,
    p.Bank_code1 AS 所属分行代码,
    (SELECT b.Bank_name FROM tb_bank b WHERE b.Bank_code1 = p.Bank_code1) AS 所属分行名称
FROM tb_risk_new r
JOIN tb_cst_pers p ON r.Cst_no = p.Cst_no
WHERE r.Risk_code = '01'  -- 高风险等级代码
  AND r.Acc_type = '11'  -- 个人账户类型
ORDER BY p.Cst_no;

-- 2. 查询企业高风险客户基本信息
SELECT
    u.Cst_no AS 客户号,
    u.Acc_name AS 企业名称,
    u.License AS 统一社会信用代码,
    u.Rep_name AS 法定代表人,
    u.Ope_name AS 经办人,
    u.Industry AS 行业,
    u.Reg_amt AS 注册资本,
    u.Reg_amt_code AS 注册资本币种,
    r.Risk_code AS 风险等级代码,
    '高风险' AS 风险等级描述,
    r.Time AS 风险评估时间,
    r.Norm AS 评估说明,
    u.Bank_code1 AS 所属分行代码,
    (SELECT b.Bank_name FROM tb_bank b WHERE b.Bank_code1 = u.Bank_code1) AS 所属分行名称
FROM tb_risk_new r
JOIN tb_cst_unit u ON r.Cst_no = u.Cst_no
WHERE r.Risk_code = '01'  -- 高风险等级代码
  AND r.Acc_type = '13'  -- 企业账户类型
ORDER BY u.Cst_no;

-- 3. 查询高风险客户的账户信息
SELECT
    '高风险客户账户' AS 数据类型,
    r.Cst_no AS 客户号,
    CASE
        WHEN p.Cst_no IS NOT NULL THEN p.Acc_name
        WHEN u.Cst_no IS NOT NULL THEN u.Acc_name
        ELSE '未知客户'
    END AS 客户名称,
    a.Self_acc_no AS 账户号,
    a.Card_no AS 卡号,
    a.Acc_type AS 账户类型,
    a.Acc_state AS 账户状态,
    a.Open_time AS 开户时间,
    a.Close_time AS 销户时间,
    r.Time AS 风险评估时间,
    r.Risk_code AS 风险等级代码,
    '高风险' AS 风险等级描述
FROM tb_risk_new r
LEFT JOIN tb_acc a ON r.Cst_no = a.Cst_no
LEFT JOIN tb_cst_pers p ON r.Cst_no = p.Cst_no
LEFT JOIN tb_cst_unit u ON r.Cst_no = u.Cst_no
WHERE r.Risk_code = '01'
ORDER BY r.Cst_no, a.Self_acc_no;

-- 4. 查询高风险客户的交易统计（2020年1-6月交易期间）
SELECT
    r.Cst_no AS 客户号,
    CASE
        WHEN p.Cst_no IS NOT NULL THEN p.Acc_name
        WHEN u.Cst_no IS NOT NULL THEN u.Acc_name
        ELSE '未知客户'
    END AS 客户名称,
    COUNT(t.Ticd) AS 交易笔数,
    COALESCE(SUM(t.Org_amt), 0) AS 交易总金额,
    COALESCE(AVG(t.Org_amt), 0) AS 平均交易金额,
    COALESCE(MAX(t.Org_amt), 0) AS 最大交易金额,
    COALESCE(MIN(t.Org_amt), 0) AS 最小交易金额,
    r.Time AS 风险评估时间,
    r.Risk_code AS 风险等级代码
FROM tb_risk_new r
LEFT JOIN tb_acc_txn t ON r.Cst_no = t.Cst_no
    AND t.Date BETWEEN '20200101' AND '20200630'  -- 交易检查期
LEFT JOIN tb_cst_pers p ON r.Cst_no = p.Cst_no
LEFT JOIN tb_cst_unit u ON r.Cst_no = u.Cst_no
WHERE r.Risk_code = '01'
GROUP BY r.Cst_no, p.Acc_name, u.Acc_name, r.Time, r.Risk_code
ORDER BY 交易总金额 DESC;

-- 5. 查询高风险客户的跨境交易
SELECT
    r.Cst_no AS 客户号,
    CASE
        WHEN p.Cst_no IS NOT NULL THEN p.Acc_name
        WHEN u.Cst_no IS NOT NULL THEN u.Acc_name
        ELSE '未知客户'
    END AS 客户名称,
    cb.Date AS 交易日期,
    cb.Ticd AS 交易流水号,
    cb.Cur AS 币种,
    cb.Org_amt AS 原币金额,
    cb.Rmb_amt AS 人民币金额,
    cb.Part_acc_name AS 交易对手,
    cb.Part_nation AS 对手国别,
    cb.Purpose AS 交易用途,
    cb.Agent_name AS 代理机构,
    r.Time AS 风险评估时间
FROM tb_risk_new r
JOIN tb_cross_border cb ON r.Cst_no = cb.Cst_no
LEFT JOIN tb_cst_pers p ON r.Cst_no = p.Cst_no
LEFT JOIN tb_cst_unit u ON r.Cst_no = u.Cst_no
WHERE r.Risk_code = '01'
  AND cb.Date BETWEEN '20200101' AND '20200630'
ORDER BY r.Cst_no, cb.Date;

-- 6. 查询高风险客户的大额交易报告
SELECT
    r.Cst_no AS 客户号,
    CASE
        WHEN p.Cst_no IS NOT NULL THEN p.Acc_name
        WHEN u.Cst_no IS NOT NULL THEN u.Acc_name
        ELSE '未知客户'
    END AS 客户名称,
    lar.Report_Date AS 报告日期,
    lar.RPMN AS 报告流水号,
    lar.Report_Amount AS 报告金额,
    lar.Currency AS 币种,
    lar.Transaction_Type AS 交易类型,
    lar.Transaction_Date AS 交易日期,
    lar.Account_No AS 账户号码,
    r.Time AS 风险评估时间
FROM tb_risk_new r
JOIN tb_lar_report lar ON (
    lar.Customer_Name = (SELECT Acc_name FROM tb_cst_pers WHERE Cst_no = r.Cst_no)
    OR lar.Customer_Name = (SELECT Acc_name FROM tb_cst_unit WHERE Cst_no = r.Cst_no)
)
LEFT JOIN tb_cst_pers p ON r.Cst_no = p.Cst_no
LEFT JOIN tb_cst_unit u ON r.Cst_no = u.Cst_no
WHERE r.Risk_code = '01'
  AND lar.Report_Date BETWEEN '20200101' AND '20200630'
ORDER BY r.Cst_no, lar.Report_Date;

-- 7. 查询高风险客户的可疑交易报告
SELECT
    r.Cst_no AS 客户号,
    CASE
        WHEN p.Cst_no IS NOT NULL THEN p.Acc_name
        WHEN u.Cst_no IS NOT NULL THEN u.Acc_name
        ELSE '未知客户'
    END AS 客户名称,
    sus.Report_Date AS 报告日期,
    sus.TICD AS 报告编号,
    sus.Transaction_Amount AS 交易金额,
    sus.Currency AS 币种,
    sus.Transaction_Type AS 交易类型,
    sus.Suspicious_Reason AS 可疑原因,
    sus.Report_Time AS 报告时间,
    r.Time AS 风险评估时间
FROM tb_risk_new r
JOIN tb_sus_report sus ON (
    sus.TICD LIKE CONCAT('%', r.Cst_no, '%') OR
    EXISTS (SELECT 1 FROM tb_cst_pers p WHERE p.Cst_no = r.Cst_no AND p.Acc_name = sus.Institution_Name) OR
    EXISTS (SELECT 1 FROM tb_cst_unit u WHERE u.Cst_no = r.Cst_no AND u.Acc_name = sus.Institution_Name)
)
LEFT JOIN tb_cst_pers p ON r.Cst_no = p.Cst_no
LEFT JOIN tb_cst_unit u ON r.Cst_no = u.Cst_no
WHERE r.Risk_code = '01'
  AND sus.Report_Date BETWEEN '20200101' AND '20200630'
ORDER BY r.Cst_no, sus.Report_Date;

-- 8. 高风险客户统计汇总
SELECT
    '统计汇总' AS 统计类型,
    COUNT(DISTINCT CASE WHEN p.Cst_no IS NOT NULL THEN r.Cst_no END) AS 个人高风险客户数,
    COUNT(DISTINCT CASE WHEN u.Cst_no IS NOT NULL THEN r.Cst_no END) AS 企业高风险客户数,
    COUNT(DISTINCT r.Cst_no) AS 高风险客户总数,
    ROUND(COUNT(DISTINCT r.Cst_no) * 100.0 / (SELECT COUNT(*) FROM tb_risk_new), 2) AS 占总客户比例
FROM tb_risk_new r
LEFT JOIN tb_cst_pers p ON r.Cst_no = p.Cst_no AND r.Acc_type = '11'
LEFT JOIN tb_cst_unit u ON r.Cst_no = u.Cst_no AND r.Acc_type = '13'
WHERE r.Risk_code = '01';

-- 9. 按分行统计高风险客户分布
SELECT
    b.Bank_name AS 分行名称,
    COUNT(DISTINCT r.Cst_no) AS 高风险客户数量,
    ROUND(COUNT(DISTINCT r.Cst_no) * 100.0 /
        (SELECT COUNT(*) FROM tb_risk_new r2
         JOIN tb_bank b2 ON r2.Bank_code1 = b2.Bank_code1
         WHERE b2.Bank_code1 = b.Bank_code1), 2) AS 分行高风险客户占比
FROM tb_risk_new r
JOIN tb_bank b ON r.Bank_code1 = b.Bank_code1
WHERE r.Risk_code = '01'
GROUP BY b.Bank_name, b.Bank_code1
ORDER BY 高风险客户数量 DESC;

-- 10. 风险等级分布统计
SELECT
    r.Risk_code AS 风险等级代码,
    CASE r.Risk_code
        WHEN '01' THEN '高风险'
        WHEN '02' THEN '中高风险'
        WHEN '03' THEN '中风险'
        WHEN '04' THEN '低风险'
        ELSE '未知'
    END AS 风险等级描述,
    COUNT(*) AS 客户数量,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM tb_risk_new), 2) AS 占比
FROM tb_risk_new r
GROUP BY r.Risk_code
ORDER BY r.Risk_code;

-- ===============================================================
-- 使用说明：
-- 1. 这些SQL查询基于AML300数据库中的实际数据
-- 2. 风险等级代码01代表高风险客户
-- 3. 数据生成时间为2020年6月30日
-- 4. 所有查询都包含客户基本信息、风险评估详情和相关业务数据
-- 5. 可根据具体需求调整查询条件，如时间范围、风险等级等
-- ===============================================================