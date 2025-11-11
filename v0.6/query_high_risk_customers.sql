-- ===============================================================
-- 查询高风险客户：2024年1月1日到2024年6月1日
-- 风险等级代码：01-高风险
-- 数据来源：tb_risk_new（最新风险等级表）
-- ===============================================================

-- 查询个人高风险客户
SELECT
    '个人客户' AS 客户类型,
    p.Cst_no AS 客户号,
    p.Acc_name AS 客户名称,
    p.Id_no AS 证件号码,
    r.Risk_code AS 风险等级代码,
    CASE r.Risk_code
        WHEN '01' THEN '高风险'
        WHEN '02' THEN '中高风险'
        WHEN '03' THEN '中风险'
        WHEN '04' THEN '低风险'
        ELSE '未知'
    END AS 风险等级描述,
    r.Time AS 评估时间,
    r.Norm AS 评估说明,
    p.Bank_code1 AS 所属分行代码,
    (SELECT b.Bank_name FROM tb_bank b WHERE b.Bank_code1 = p.Bank_code1) AS 所属分行名称,
    p.Contact1 AS 联系方式,
    p.Occupation AS 职业,
    p.Income AS 年收入
FROM tb_risk_new r
JOIN tb_cst_pers p ON r.Cst_no = p.Cst_no
WHERE r.Risk_code = '01'  -- 高风险
  AND r.Time BETWEEN '20240101' AND '20240601'
  AND r.Acc_type = '11'  -- 个人账户类型

UNION ALL

-- 查询企业高风险客户
SELECT
    '企业客户' AS 客户类型,
    u.Cst_no AS 客户号,
    u.Acc_name AS 客户名称,
    u.License AS 证件号码,
    r.Risk_code AS 风险等级代码,
    CASE r.Risk_code
        WHEN '01' THEN '高风险'
        WHEN '02' THEN '中高风险'
        WHEN '03' THEN '中风险'
        WHEN '04' THEN '低风险'
        ELSE '未知'
    END AS 风险等级描述,
    r.Time AS 评估时间,
    r.Norm AS 评估说明,
    u.Bank_code1 AS 所属分行代码,
    (SELECT b.Bank_name FROM tb_bank b WHERE b.Bank_code1 = u.Bank_code1) AS 所属分行名称,
    u.Rep_name AS 联系人,
    u.Industry AS 行业,
    u.Reg_amt AS 注册资本
FROM tb_risk_new r
JOIN tb_cst_unit u ON r.Cst_no = u.Cst_no
WHERE r.Risk_code = '01'  -- 高风险
  AND r.Time BETWEEN '20240101' AND '20240601'
  AND r.Acc_type = '13'  -- 企业账户类型

ORDER BY 客户类型, 评估时间 DESC;

-- ===============================================================
-- 统计查询：高风险客户数量统计
-- ===============================================================

-- 按客户类型统计高风险客户数量
SELECT
    '个人客户' AS 客户类型,
    COUNT(*) AS 高风险客户数量,
    AVG(p.Income) AS 平均年收入
FROM tb_risk_new r
JOIN tb_cst_pers p ON r.Cst_no = p.Cst_no
WHERE r.Risk_code = '01'
  AND r.Time BETWEEN '20240101' AND '20240601'
  AND r.Acc_type = '11'

UNION ALL

SELECT
    '企业客户' AS 客户类型,
    COUNT(*) AS 高风险客户数量,
    AVG(u.Reg_amt) AS 平均注册资本
FROM tb_risk_new r
JOIN tb_cst_unit u ON r.Cst_no = u.Cst_no
WHERE r.Risk_code = '01'
  AND r.Time BETWEEN '20240101' AND '20240601'
  AND r.Acc_type = '13';

-- ===============================================================
-- 扩展查询：高风险客户的交易行为分析
-- ===============================================================

-- 查询高风险客户在2024年1-6月的交易统计
SELECT
    p.Cst_no AS 客户号,
    p.Acc_name AS 客户名称,
    COUNT(t.Ticd) AS 交易笔数,
    SUM(t.Org_amt) AS 交易总金额,
    AVG(t.Org_amt) AS 平均交易金额,
    MAX(t.Org_amt) AS 最大交易金额,
    MIN(t.Org_amt) AS 最小交易金额
FROM tb_risk_new r
JOIN tb_cst_pers p ON r.Cst_no = p.Cst_no
LEFT JOIN tb_acc_txn t ON p.Cst_no = t.Cst_no
    AND t.Date BETWEEN '20240101' AND '20240601'
WHERE r.Risk_code = '01'
  AND r.Time BETWEEN '20240101' AND '20240601'
  AND r.Acc_type = '11'
GROUP BY p.Cst_no, p.Acc_name
ORDER BY 交易总金额 DESC;

-- ===============================================================
-- 跨境交易查询：高风险客户的跨境交易情况
-- ===============================================================

-- 查询高风险客户的跨境交易
SELECT
    p.Cst_no AS 客户号,
    p.Acc_name AS 客户名称,
    cb.Date AS 交易日期,
    cb.Ticd AS 交易流水号,
    cb.Cur AS 币种,
    cb.Org_amt as 原币金额,
    cb.Rmb_amt AS 人民币金额,
    cb.Part_acc_name AS 交易对手,
    cb.Part_nation AS 对手国别,
    cb.Purpose AS 交易用途
FROM tb_risk_new r
JOIN tb_cst_pers p ON r.Cst_no = p.Cst_no
JOIN tb_cross_border cb ON p.Cst_no = cb.Cst_no
WHERE r.Risk_code = '01'
  AND r.Time BETWEEN '20240101' AND '20240601'
  AND r.Acc_type = '11'
  AND cb.Date BETWEEN '20240101' AND '20240601'
ORDER BY p.Cst_no, cb.Date;